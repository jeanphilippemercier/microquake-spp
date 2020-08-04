import argparse
import subprocess

parser = argparse.ArgumentParser(description='copy a directory to multiple '
                                             'pods given a basename')

parser.add_argument('-f', '--file', help='file or directory to copy')
parser.add_argument('-d', '--dest', help='destination')
parser.add_argument('-n', '--name', help='deployment name')

args = parser.parse_args()

cmd = 'kubectl get pods'
process = subprocess.run(cmd.split(), text=True, stdout=subprocess.PIPE)
pod_list = process.stdout.split('\n')[1:-1]

cmd1 = 'tar cf - spp '
for pod in pod_list:
    if 'event-connector-test-scale' in pod:
        pod_name = pod.split()[0]
        cmd2 = f'kubectl exec -i {pod_name} -- tar xf - -C /app/'
        print(f'{pod_name}')
        ps = subprocess.Popen(cmd1.split(), stdout=subprocess.PIPE)
        output = subprocess.check_output(cmd2.split(), stdin=ps.stdout)






