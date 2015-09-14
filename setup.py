import versioneer
from setuptools import setup, find_packages

versioneer.VCS = 'git'
versioneer.versionfile_source = 'gbatchy/_version.py'
versioneer.versionfile_build = 'gbatchy/_version.py'
versioneer.tag_prefix = ''
versioneer.parentdir_prefix = 'gbatchy-'

setup(
    name='gbatchy',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='gevent based batching framework',
    long_description='https://github.com/mikekap/gbatchy',
    url='https://github.com/mikekap/gbatchy',
    author='Mike Kaplinskiy',
    author_email='mike.kaplinskiy@gmail.com',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=[
        'gevent>1.0.9',
        'ProxyTypes>=0.9',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
    ]
)
