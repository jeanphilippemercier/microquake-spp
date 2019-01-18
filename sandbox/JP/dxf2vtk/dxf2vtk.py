from dxfgrabber import readfile
import os
import vtk
import argparse
from glob import glob

parser = argparse.ArgumentParser(description='Convert polyline contained in '
                                             'dxf into vtp')

parser.add_argument('input_file', type=str, help='input file name, can be '
                                                 'wild card as glob is used')

args = parser.parse_args()

print(args.input_file)
for input_file in glob(args.input_file):
    if input_file.split('.')[-1] != 'dxf':
        continue
    print(input_file)

    dxf = readfile(input_file)

    pid = 0
    points = vtk.vtkPoints()
    lines = vtk.vtkCellArray()
    for entity in dxf.entities:
        if entity.dxftype == 'POLYLINE':
            # points = []
            line = []
            for pt in entity.points:
                pid = points.InsertNextPoint(pt)
                # points.append(np.array(pt))
                line.append(pid)

            lines.InsertNextCell(len(line))
            for l in line:
                lines.InsertCellPoint(l)

    polygon = vtk.vtkPolyData()
    polygon.SetPoints(points)
    polygon.SetLines(lines)

    writer = vtk.vtkXMLPolyDataWriter();
    writer.SetFileName(input_file.replace('.dxf', '.vtp'));
    # if vtk.VTK_MAJOR_VERSION <= 5:
    #     writer.SetInput(polydata)
    # else:
    writer.SetInputData(polygon)
    writer.Write()
