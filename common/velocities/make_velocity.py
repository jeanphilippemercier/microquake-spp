from microquake.core import GridData
import os
from microquake.core import ctl
import numpy as np
# import matplotlib.pyplot as plt
from microquake.core.data.grid import read_grid

config_dir = os.environ['SPP_CONFIG']
config_file = config_dir + '/microquake.xml'

z = [1168, 459, -300]
Vp_z = [4533, 5337, 5836]
Vs_z = [2306, 2885, 3524]

params = ctl.parse_control_file(config_file)

vp = params.velgrids.grids['vp']
vs = params.velgrids.grids['vs']

zis = [int(vp.transform_to([params.origin[0], params.origin[1], z_])[2]) for z_ in z]

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

# plt.imshow(vs.data[:,40, -1::-1].T, extent=[x[0], x[-1], z[0], z[-1]])
# plt.colorbar()
# plt.show()

vp.write('vp.pickle', format='PICKLE')
vs.write('vs.pickle', format='PICKLE')


