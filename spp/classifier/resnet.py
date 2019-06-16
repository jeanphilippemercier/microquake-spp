import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import confusion_matrix

import h5py
import keras
# import librosa as lr
from keras import Model
from keras.callbacks import (EarlyStopping, LearningRateScheduler,
                             ModelCheckpoint, ReduceLROnPlateau)
from keras.layers import (Activation, Add, BatchNormalization, Concatenate,
                          Conv2D, Dense, Dropout, Flatten, Input, Lambda,
                          MaxPooling2D, Multiply)
from keras.losses import mean_absolute_error, mean_squared_error
from keras.models import Sequential, load_model, model_from_json
from keras.optimizers import Adam, RMSprop
from keras.preprocessing.image import ImageDataGenerator
from keras.utils import Sequence
from loguru import logger


class Resnet:
    '''
    Class to classify mseed stream into one of the classes
    Blast UG, Blast OP, Blast C2S, Seismic, Noise
    '''

    def __enter__(self):
        return (self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, lr=0.01, batch_size=16, epochs=100):
        self.base_directory = Path(os.path.dirname(os.path.realpath(__file__)))
        self.data_directory = self.base_directory/'output'
        data_file = self.data_directory/'train_test_data/data.h5'
        train_test = h5py.File(data_file)
        logger.debug(train_test.name)
        self.x_test = train_test['/x_test'][()]
        self.x_train = train_test['/x_train'][()]
        self.y_test = train_test['/y_test'][()].astype(int)
        self.y_train = train_test['/y_train'][()].astype(int)

        # self.x_test = np.hstack([self.x_test, np.zeros(self.x_test.shape)])[
        #     :, :self.x_test.shape[2], :]
        # self.x_train = np.hstack([self.x_train, np.zeros(self.x_train.shape)])[
        #     :, :self.x_train.shape[2], :]
        self.D = self.x_train.shape[1:]
        self.num_classes = len(np.unique(self.y_train))
        self.x_test = self.x_test.reshape(-1, self.D[0], self.D[1], 1)
        self.x_train = self.x_train.reshape(-1, self.D[0], self.D[1], 1)
        self.D = self.x_train.shape[1:]
        self.batch_size = batch_size
        self.epochs = epochs
        self.lr = lr
        self.class_names = ['Blast UG', 'Blast OP',
                            'Blast C2S', 'Seismic', 'Noise']
        self.model_file = self.data_directory/'model.hdf5'
        self.model_file = f"{self.model_file.absolute()}"
        logger.info(
            f'Train dataset: {self.x_train.shape}, test dataset: {self.x_test.shape}')

    def create_model(self):
        i = Input(shape=self.D, name="input")
        dim = 128

        n_res = 2
        kern_size = (3, 3)

        dim = 64
        x = Conv2D(filters=16, kernel_size=2,
                   padding='same', activation='relu')(i)
        x = MaxPooling2D(pool_size=2)(x)
        x = Conv2D(filters=32, kernel_size=2,
                   padding='same', activation='relu')(x)
        x = MaxPooling2D(pool_size=2)(x)
        x = Conv2D(filters=64, kernel_size=2,
                   padding='same', activation='relu')(x)
        x = MaxPooling2D(pool_size=2)(x)
        x = Dropout(0.3)(x)

        X_shortcut = BatchNormalization()(x)

        for _ in range(n_res):
            y = Conv2D(filters=dim, kernel_size=kern_size,
                       activation='relu', padding='same')(X_shortcut)
            y = BatchNormalization()(y)
            # y = Conv2D(filters=dim, kernel_size=kern_size,
            #            activation='relu', padding='same')(y)
            y = Conv2D(filters=dim, kernel_size=kern_size,
                       activation='relu', padding='same')(y)

            X_shortcut = Add()([y, X_shortcut])

        x = Flatten()(x)
        x = Dense(500, activation='relu')(x)
        x = Dropout(0.4)(x)
        x = Dense(self.num_classes, activation='softmax')(x)
        self.model = Model(i, x)
        self.model.compile(loss=keras.losses.sparse_categorical_crossentropy,
                           optimizer=keras.optimizers.adam(self.lr), metrics=['accuracy'])
        logger.debug(self.model.summary())

    #############################################
    # Modeling
    #############################################

    def plot_confusion_matrix(self, y_true, y_pred, classes,
                              normalize=False,
                              title=None,
                              cmap=plt.cm.Blues):
        """
        This function prints and plots the confusion matrix.
        Normalization can be applied by setting `normalize=True`.
        """

        if not title:
            if normalize:
                title = 'Normalized confusion matrix'
            else:
                title = 'Confusion matrix, without normalization'

        # Compute confusion matrix
        cm = confusion_matrix(y_true, y_pred)
        # Only use the labels that appear in the data
        classes = classes

        if normalize:
            cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
            logger.info("Normalized confusion matrix")
        else:
            logger.info('Confusion matrix, without normalization')

        logger.info(f"\n{cm}\n")

        fig, ax = plt.subplots(figsize=(5, 5))
        im = ax.imshow(cm, interpolation='nearest', cmap=cmap)
        ax.figure.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        # We want to show all ticks...
        ax.set(xticks=np.arange(cm.shape[1]),
               yticks=np.arange(cm.shape[0]),
               # ... and label them with the respective list entries
               xticklabels=classes, yticklabels=classes,
               title=title,
               ylabel='True label',
               xlabel='Predicted label')

        # Rotate the tick labels and set their alignment.
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
                 rotation_mode="anchor")

        # Loop over data dimensions and create text annotations.
        fmt = '.2f' if normalize else 'd'
        thresh = cm.max() / 2.

        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, format(cm[i, j], fmt),
                        ha="center", va="center",
                        color="white" if cm[i, j] > thresh else "black")
        fig.tight_layout()
        # plt.show()
        plt.savefig(self.data_directory/'cm.png')

        return ax

    @staticmethod
    def learning_rate_schedule(e):
        inc_rate = np.geomspace(0.00005, 0.008, num=10)
        dec_rate = np.geomspace(0.008, 0.00005, num=40)

        if e < 300:
            j = e % 50

            if j < 10:
                return inc_rate[j]
            elif j < 50:
                return dec_rate[j-10]

        return 0.000025

    def fit(self):
        lossName = "val_acc"
        callbacks = [
            # EarlyStopping(monitor=lossName, patience=100, verbose=1, min_delta=0,
            #               mode='max', baseline=0.85, restore_best_weights=True),
            # ReduceLROnPlateau(monitor=lossName, factor=0.2,
            #                   patience=5, verbose=1, min_delta=1e-4,  mode='min'),
            ModelCheckpoint(filepath=self.model_file,
                            verbose=0, save_best_only=True),
            LearningRateScheduler(self.learning_rate_schedule, verbose=2)
        ]

        # create and configure augmented image generator
        datagen = ImageDataGenerator(
            # randomly shift images horizontally (20% of total width)
            width_shift_range=0.2,
            # randomly shift images vertically (1% of total height)
            height_shift_range=0.01,
            horizontal_flip=False)  # randomly flip images horizontally

        self.model.fit_generator(datagen.flow(self.x_train, self.y_train, batch_size=self.batch_size),
                                 epochs=self.epochs, steps_per_epoch=self.x_train.shape[
                                     0]//self.batch_size,
                                 verbose=1, validation_data=(self.x_test, self.y_test), callbacks=callbacks)

        # self.model.save_weights(self.model_file)
        self.model.load_weights(self.model_file)
        y_pred = self.model.predict(self.x_test)
        y_pred_ = np.argmax(y_pred, axis=1)
        accuracy = np.mean(y_pred_ == self.y_test)
        logger.info(f'Model accuracy: {accuracy}')
        self.plot_confusion_matrix(
            self.y_test, y_pred_, self.class_names, title='Confusion Matrix')
