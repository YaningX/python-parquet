from Cython.Distutils import build_ext
from distutils.core import setup
from distutils.extension import Extension

ext_modules = [
    Extension("pyparquet",
              ["pyparquet.pyx"],
              libraries=["stdc++", "materializing_reader", "parquet"],
              language="c++",
              include_dirs=["../materializing_reader/include"],
              cmdclass={"build_ext": build_ext},
              library_dirs=["../build"],
              extra_compile_args=['-std=c++11']
              )
]

setup(name="pyparquet", ext_modules=ext_modules, cmdclass={"build_ext": build_ext})
