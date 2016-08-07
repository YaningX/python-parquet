from Cython.Distutils import build_ext
from distutils.core import setup, Command
from distutils.extension import Extension
import os, os.path
import subprocess

ext_modules = [
    Extension("_pyparquet",
              ["_pyparquet.pyx"],
              libraries=["stdc++", "parquet-record-reader", "parquet" ],
              language="c++",
              include_dirs=["../parquet-record-reader/include"],
              library_dirs=["../build/Debug"],
              extra_compile_args=['-std=c++11']
              )
]


class GenerateThriftCommand(Command):
    description = 'generate python file from thrift sources'
    user_options = []
    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):
        print "Generating files from thrift schema"
        subprocess.call(['thrift', '-o', './pyparquet/thrift', '--gen', 'py', './pyparquet/thrift/parquet.thrift'])



setup(name="pyparquet",
      ext_modules=ext_modules,
      cmdclass={"build_ext": build_ext, 'thrift': GenerateThriftCommand},
      packages=['pyparquet'],
      package_data={'pyparquet': ['data/*.thrift']},
      version='0.1.0b1',
      description="Python parquet reader",
      author="Roman Bystritskiy",
      author_email="rbystrit@gmail.com",
      url="https://github.com/rbystrit/pyparquet",
      license='License :: OSI Approved :: Apache Software License',
      classifiers=[
          # How mature is this project? Common values are
          #   3 - Alpha
          #   4 - Beta
          #   5 - Production/Stable
          'Development Status :: 4 - Beta',

          # Indicate who your project is intended for
          'Intended Audience :: Developers',

          # Pick your license as you wish (should match "license" above)
          'License :: OSI Approved :: Apache Software License',

          # Specify the Python versions you support here. In particular, ensure
          # that you indicate whether you support Python 2, Python 3 or both.
          'Programming Language :: Python :: 2.7',
      ],
      keywords='parquet hadoop hdfs',
      requires=['thrift (>=0.9)'],
      )
