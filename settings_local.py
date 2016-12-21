import os

# Define ENVIRONMENTAL VARIABLES for project (replaces the app.yaml)
os.environ.update({
    'CTS_TEST_SERVER': 'http://172.20.100.16:8080',
    'CTS_EPI_SERVER': 'http://172.20.100.18',
    'CTS_EFS_SERVER': 'http://172.20.100.12',
    'CTS_JCHEM_SERVER': 'http://172.20.100.12',
    'CTS_SPARC_SERVER': 'http://204.46.160.69:8080',
})
