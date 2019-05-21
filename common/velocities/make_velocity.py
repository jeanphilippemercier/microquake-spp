import numpy as np

from microquake.core.data.grid import create
from spp.utils.application import Application

app = Application()

settings = app.settings

z = [1168, 459, -300]
Vp_z = [4533, 5337, 5836]
Vs_z = [2306, 2885, 3524]

vp = create(**app.grids)
vs = create(**app.grids)

origin = app.grids.origin

zis = [int(vp.transform_to([origin[0], origin[1], z_])[2]) for z_ in z]

vp.data[:,:,zis[0]:] = Vp_z[0]
vs.data[:,:,zis[0]:] = Vs_z[0]

vp.data[:,:,zis[1]:zis[0]] = np.linspace(Vp_z[1], Vp_z[0], zis[0] - zis[1])
vs.data[:,:,zis[1]:zis[0]] = np.linspace(Vs_z[1], Vs_z[0], zis[0] - zis[1])

vp.data[:,:,zis[2]:zis[1]] = np.linspace(Vp_z[2], Vp_z[1], zis[1] - zis[2])
vs.data[:,:,zis[2]:zis[1]] = np.linspace(Vs_z[2], Vs_z[1], zis[1] - zis[2])

vp.data[:,:,:zis[2]] = Vp_z[2]
vs.data[:,:,:zis[2]] = Vs_z[2]

(lx, ly, lz) = vp.shape

x = [vp.transform_from(np.array([x_, 0, 0]))[0] for x_ in range(0, lx)]
y = [vp.transform_from(np.array([0, y_, 0]))[1] for y_ in range(0, ly)]
z = [vp.transform_from(np.array([0, 0, z_]))[2] for z_ in range(0, lz)]

vp.write('vp', format='NLLOC')
vs.write('vs', format='NLLOC')

with open('vp.rid', 'w') as vp:
    vp.write('initial_1d_vp_velocity_model_2018_01')

with open('vs.rid', 'w') as vs:
    vp.write('initial_1d_vs_velocity_model_2018_01')

