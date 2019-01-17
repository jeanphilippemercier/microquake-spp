from spp.utils.application import Application
from microquake.simul import eik
import matplotlib.pyplot as plt
from importlib import reload
import numpy as np

reload(eik)
app = Application()
tt_grid = app.get_grid(20, 'P')

# array([61.20536, 32.686  , 47.13392])

start = [57, 34, 47]

ray = eik.ray_tracer(tt_grid, start, grid_coordinates=True)

origin = tt_grid.origin
spc = tt_grid.spacing
shape = np.array(tt_grid.shape)

plt.clf()

plt.imshow(tt_grid.data[:,:,47].T, aspect='auto',
           extent=[origin[0], origin[0] + spc * shape[0],
                   origin[1], origin[1] + spc * shape[1]],
           origin='lower')

plt.plot(tt_grid.seed[0], tt_grid.seed[1], 'k*')

plt.plot(ray.nodes[:,0], ray.nodes[:,1], 'k')
plt.plot([ray.nodes[0,0], ray.nodes[-1,0]],
         [ray.nodes[0,1], ray.nodes[-1,1]],
         ':r')

plt.show()



