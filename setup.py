from setuptools import setup, find_packages

setup(
  name='waldo-photos',
  version='1.0.0',
  description='Waldo Photos Test',
  url='https://www.github.com/aztecapps/waldo-photos',
  author='David Seemiller',
  author_email='david.j.seemiller@gmail.com',
  license='MIT',
  packages=find_packages(),
  install_requires=['Pillow', 'requests', 'lxml', 'pymongo', 'argparse'],
  entry_points={
    'console_scripts': [
      'waldophotos=waldo_photos.waldo_photos:main'
    ]
  }
)
