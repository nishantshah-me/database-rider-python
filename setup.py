from setuptools import find_packages, setup
setup(
    name='dbrider',
    packages=find_packages(include=['src/dbrider']),
    version='1.0.1',
    description='Database Rider for Python',
    author='Nishant Shah',
    license='Apache License 2.0',
    install_requires=[
        'PyYAML==6.0',
        'peewee==3.14.10'
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==7.1.1', 'pytest-mock==3.7.0', 'pytest-cov==3.0.0'],
    test_suite='tests'
)
