from pathlib import Path
import os
import matplotlib.pyplot as plt
import numpy as np
from keras.models import Model
from keras.layers import (Add, BatchNormalization, Conv2D, Dense, Flatten, Input, concatenate,
                          MaxPooling2D, Embedding)
from loguru import logger
import librosa as lr
class SeismicClassifierModel:
    '''
    Class to classify mseed stream into one of the classes
        anthropogenic event, controlled explosion, earthquake, explosion, quarry blast
    '''
    REF_HEIGHT = 900.00
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, model_name='multiclass-model.hdf5'):
        '''
            :param model_name: Name of the model weight file name.            
        '''
        self.base_directory = Path(os.path.dirname(os.path.realpath(__file__)))/'../../data/weights'
        #Model was trained at these dimensions
        self.D = (128, 128, 1)
        self.microquake_class_names = ['anthropogenic event',
                                       'controlled explosion',
                                       'earthquake',
                                       'explosion',
                                       'quarry blast']
        self.num_classes = len(self.microquake_class_names)
        self.model_file = self.base_directory/f"{model_name}"
        self.create_model()

    ###############################################
    # Librosa gives mel Spectrum which shows events clearly,
    # it is designed for audio frequencies which is suitable
    # to seismic events
    ################################################
    def librosa_spectrogram(self, tr, height=128, width=128):
        '''
            Using Librosa mel-spectrogram to obtain the spectrogram
            :param tr: stream trace
            :param height: image hieght
            :param width: image width
            :return: numpy array of spectrogram with height and width dimension
        '''
        data = self.get_norm_trace(tr).data
        signal = data*255
        hl = int(signal.shape[0]//(width*1.1)) #this will cut away 5% from start and end
        spec = lr.feature.melspectrogram(signal, n_mels=height,
                                         hop_length=int(hl))
        img = lr.amplitude_to_db(spec)
        start = (img.shape[1] - width) // 2
        return img[:, start:start+width]

    #############################################
    # Data preparation
    #############################################
    @staticmethod
    def get_norm_trace(tr, taper=True):
        """
        :param tr: mseed stream
        :param taper: Boolean
        :return: normed composite trace
        """

        # c = tr[0]
        c = tr.composite()
        c.data = c / np.abs(c).max()
        c = c.detrend(type='demean')

        nan_in_context = np.any(np.isnan(c[0].data))

        logger.info('is there any nan in the context trace {}'.format(
            nan_in_context))

        if taper:
            c = c.taper(max_percentage=0.05)

        return c[0]

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
        input_shape = (self.D[0], self.D[1], 1)
        i1 = Input(shape=input_shape, name="spectrogram")
        i2 = Input(shape=(1,), name='hour', dtype='int32')
        i3 = Input(shape=(1,), name='height', dtype='int32')
        emb = Embedding(24, 50)(i2) #24 hours to 12 hours
        flat1 = Flatten()(emb)
        emb = Embedding(2,50)(i3)
        flat2 = Flatten()(emb)
        dim = 128
        n_res = 2
        kern_size = (3, 3)

        dim = 64
        x = Conv2D(filters=16, kernel_size=2, padding='same',
                   activation='relu')(i1)
        x = MaxPooling2D(pool_size=2)(x)
        x = Conv2D(filters=32, kernel_size=2, padding='same',
                   activation='relu')(x)
        x = MaxPooling2D(pool_size=2)(x)
        x = Conv2D(filters=64, kernel_size=2, padding='same',
                   activation='relu')(x)
        x = MaxPooling2D(pool_size=2)(x)
        #x = Dropout(0.3)(x) # not needed to do inference
        X_shortcut = BatchNormalization()(x)
        for _ in range(n_res):
            y = Conv2D(filters=dim, kernel_size=kern_size, activation='relu',
                       padding='same')(X_shortcut)
            y = BatchNormalization()(y)
            #y = Conv2D(filters = dim, kernel_size = kern_size, activation='relu', padding='same')(y)
            y = Conv2D(filters=dim, kernel_size=kern_size, activation='relu',
                       padding='same')(y)
            X_shortcut = Add()([y,X_shortcut])
        x = Flatten()(x)
        #x = Dropout(0.4)(x) # not needed to do inference
        x = Dense(500, activation='relu')(x)
        x = concatenate([x, flat1, flat2], axis=-1)
        x = BatchNormalization()(x)
        x = Dense(128, activation='relu')(x)
        x = Dense(self.num_classes, activation='sigmoid')(x)
        self.model = Model([i1, i2, i3], x)
        self.model.load_weights(self.model_file)

 
    def predict(self, tr, height):
        """
        :param tr: Obspy stream object
        :param height: the z-value of event.
        :return: dictionary of  event classes probability
        """
        hour = tr[0].stats.starttime.hour
        spectrogram = self.librosa_spectrogram(tr)
        #graygram = self.rgb2gray(spectrogram)
        normgram = self.normalize_gray(spectrogram)
        img = normgram[None, ..., None] # Needed to in the form of batch with one channel.
        h = []
        #We use the height as a category, greater than 900 or not
        if height >= SeismicClassifierModel.REF_HEIGHT:
            h.append(1)
        else:
            h.append(0)
        data = {'spectrogram': img, 'hour':np.asarray([hour]),
                'height': np.asarray(h)}
        a = self.model.predict(data)
        
        classes = {}
        for p, n in zip(a.reshape(-1), self.microquake_class_names):
            classes[n] = p
        classes['other event'] = 1-np.max(a.reshape(-1))
        return classes
