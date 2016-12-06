import os


# Define ENVIRONMENTAL VARIABLES for project (replaces the app.yaml)
os.environ.update({
    # 'CTS_TEST_SERVER': 'http://172.20.100.16:8080',
    'CTS_TEST_SERVER': '',  # currently not available locally
    'CTS_EPI_SERVER': 'http://localhost:55342',
    'CTS_EFS_SERVER': 'http://ca-test-1.cloudapp.net',
    'CTS_JCHEM_SERVER': 'http://ca-test-1.cloudapp.net',
    'CTS_SPARC_SERVER': 'http://204.46.160.69:8080',
})