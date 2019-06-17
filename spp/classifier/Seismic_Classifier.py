import os
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from keras.models import Model
from keras.layers import (Add, BatchNormalization, Conv2D, Dense, Dropout, Flatten, Input, 
                          MaxPooling2D)
class SeismicClassifierModel:
    '''
    Class to classify mseed stream into one of the classes
    Blast UG, Blast OP, Blast C2S, Seismic, Noise
    '''

    def __enter__(self):
        return (self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, model_name = 'best_model.hdf5'):
        '''
            :param model_name: Name of the model weight file name.            
        '''
        self.base_directory = Path(os.path.dirname(os.path.realpath(__file__)))
        #Model was trained at these dimensions
        self.D = (64, 64, 1)
        self.class_names = ['Blast UG', 'Blast OP', 'Blast C2S', 'Seismic', 'Noise']
        self.num_classes = len(self.class_names)
        self.model_file = self.base_directory/f"{model_name}"
        self.create_model()  

            
    #############################################
    # Data preparation
    #############################################
    @staticmethod
    def get_norm_trace(tr, taper=True):
        """
        :param tr: mseed stream
        :param taper: Boolean
        :return: 1. Combine x, y, z
                2. Standardize to ~ [-1, 1]
                3. Detrend & Taper
        """        
        x = tr[0].data
        y = tr[1].data
        z = tr[2].data

        c = np.sign(x) * np.sqrt(x ** 2 + y ** 2 + z ** 2)
        c_norm = c / np.abs(c).max()
        tr[0].data = c_norm

        tr[0] = tr[0].detrend(type='demean')

        if taper:
            tr[0] = tr[0].taper(max_percentage=0.05)

        return tr[0]

    @staticmethod
    def get_spectrogram(tr, nfft=512, noverlap=511):
        """
        :param tr: mseed stream
        :param nfft: The number of data points used in each block for the FFT
        :param noverlap: The number of points of overlap between blocks
        :param output_dir: directory to save spectrogram.png such as SPEC_BLAST_UG
        :return: RBG image array
        """
        rate = SeismicClassifierModel.get_norm_trace(tr).stats.sampling_rate
        data = SeismicClassifierModel.get_norm_trace(tr).data

        fig, ax = plt.subplots(1)
        fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        ax.axis('off')
        _, _, _, _ = ax.specgram(x=data, Fs=rate,
                                        noverlap=noverlap, NFFT=nfft)
        ax.axis('off')
        fig.set_size_inches(.64, .64)
        
        fig.canvas.draw()
        size_inches = fig.get_size_inches()
        dpi = fig.get_dpi()
        width, height = fig.get_size_inches() * fig.get_dpi()
        mplimage = np.frombuffer(fig.canvas.tostring_rgb(), dtype=np.uint8)

        imarray = np.reshape(mplimage, (int(height), int(width), 3))
        plt.close(fig)
        return imarray

    @staticmethod
    def rgb2gray(rgb):
        """
        Convert RBG colored image to gray scale
        :param rgb: RGB image array
        :return: Gray scaled image array
        """
        return np.dot(rgb[..., :3], [0.299, 0.587, 0.114])

    @staticmethod
    def normalize_gray(array):
        """
        :param array: Gray colored image array
        :return: Normalized gray colored image
        """
        return (array - array.min()) / (array.max() - array.min())

    def create_model(self):
        """
        Create model and load weights
        """
        i = Input(shape=self.D, name="input")
        dim = 128
        n_res = 2
        kern_size = (3, 3)

        dim = 64
        x = Conv2D(filters=16, kernel_size=2, padding='same', activation='relu')(i)
        x = MaxPooling2D(pool_size=2)(x)
        x = Conv2D(filters=32, kernel_size=2, padding='same', activation='relu')(x)
        x = MaxPooling2D(pool_size=2)(x)
        x = Conv2D(filters=64, kernel_size=2, padding='same', activation='relu')(x)
        x = MaxPooling2D(pool_size=2)(x)
        x = Dropout(0.3)(x)
              
        X_shortcut = BatchNormalization()(x)
        for _ in range(n_res):
            y = Conv2D(filters = dim, kernel_size = kern_size, activation='relu', padding='same')(X_shortcut)
            y = BatchNormalization()(y)
            #y = Conv2D(filters = dim, kernel_size = kern_size, activation='relu', padding='same')(y)
            y = Conv2D(filters = dim, kernel_size = kern_size, activation='relu', padding='same')(y)
            
            X_shortcut = Add()([y,X_shortcut])
            
        x = Flatten()(x)
        x = Dense(500, activation='relu')(x)
        x = Dropout(0.4)(x)
        x = Dense(self.num_classes, activation='softmax')(x)
        self.model = Model(i,x)
        self.model.load_weights(self.model_file)

    def predict(self, tr):
        """
        :param tr: Obspy stream object
        :return: Normalized gray colored image
        """
        spectrogram = self.get_spectrogram(tr)
        graygram = self.rgb2gray(spectrogram)
        normgram = self.normalize_gray(graygram)
        img = normgram[None,...,None]
        a =  self.model.predict(img)
        return self.class_names[np.argmax(a)]