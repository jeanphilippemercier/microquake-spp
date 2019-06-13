import sys
import numpy as np
import pandas as pd
from obspy.core import read, Stream
import obspy
from pathlib import Path
import librosa as lr
import keras
from loguru import logger
from keras.layers import Dense, Conv2D, Dropout, Flatten, MaxPooling2D
from keras.models import model_from_json
from keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint, LearningRateScheduler
from keras import Model
from keras.layers import Input, Dense, Dropout, BatchNormalization
from keras.layers import Concatenate, Add, Activation, Multiply, Lambda
from keras.optimizers import Adam, RMSprop
from keras.losses import mean_squared_error, mean_absolute_error
from keras.models import load_model
from keras.utils import Sequence
import keras.backend as K
import h5py
from sklearn.base import BaseEstimator, ClassifierMixin
from .processing_unit import ProcessingUnit
'''
    Class to classify mseed stream into one of the classes
    Blast UG, Blast OP, Blast C2S, Seismic, Noise
'''
class event_classifier(BaseEstimator, ClassifierMixin, ProcessingUnit):
    def __enter__(self):
        return (self)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def __init__(self, logger: logger):
        '''
            Initialise the event classfier class
        '''
        self.log = logger
        self.name = 'share_selector'
        self.class_names = ['Blast UG', 'Blast OP', 'Blast C2S', 'Seismic', 'Noise']
        

    def fit(self, X, y):
        '''
            Train the model using X and y data
        '''
        pass
    
    def score(self, X, y = None):
        '''
        Score the model using test data
        '''
        pass

    def predict(self, mseed: Stream, y:str = None):
        '''
            Predict the classification of mseed stream.
        '''
        return 'None'