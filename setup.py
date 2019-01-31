from setuptools import setup, Extension

module1 = Extension("pb",
                    sources=["pb.c", "deviceapps.pb-c.c"],
                    extra_compile_args=["-g", "-std=c99", "-DHAVE_ZLIB=1"],
                    libraries=["protobuf-c"],
                    library_dirs=["/usr/lib"],
                    include_dirs=["/usr/include/google/protobuf-c/"],
                    extra_link_args=['-lz'],
                    )

setup(name="pb",
      version="1.0",
      description="Protobuf (de)serializer",
      test_suite="tests",
      ext_modules=[module1])
