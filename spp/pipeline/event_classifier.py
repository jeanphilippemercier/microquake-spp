import sys
from pathlib import Path

import numpy as np
from sklearn.base import BaseEstimator, ClassifierMixin

import h5py
import keras
import keras.backend as K
import librosa as lr
import obspy
import pandas as pd
from keras import Model
from keras.callbacks import (EarlyStopping, LearningRateScheduler,
                             ModelCheckpoint, ReduceLROnPlateau)
from keras.layers import (Activation, Add, BatchNormalization, Concatenate,
                          Conv2D, Dense, Dropout, Flatten, Input, Lambda,
                          MaxPooling2D, Multiply)
from keras.losses import mean_absolute_error, mean_squared_error
from keras.models import load_model, model_from_json
from loguru import logger

from ..classifer.resnet import Resnet
from .processing_unit import ProcessingUnit


class Processor(ProcessingUnit):
    @property
    def module_name(self):
        return "event_classifier"

    def initializer(self):
        self.resnet = Resnet(lr=0.0001, batch_size=50, epochs=1000)

    def process(
        self,
        **kwargs
    ):
        stream = kwargs["stream"]

        self.resnet.create_model()
        self.resnet.fit()

        return self.response

    def legacy_pipeline_handler(
        self,
        msg_in,
        res
    ):
        cat, stream = self.app.deserialise_message(msg_in)

        cat = self.output_catalog(cat)

        return cat, stream
