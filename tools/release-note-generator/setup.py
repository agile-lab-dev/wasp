import pip
try: # for pip >= 10
   from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
   from pip.req import parse_requirements
try: # for pip >= 10
  from pip._internal import download
except ImportError: # for pip <= 9.0.3
  from pip import download
from setuptools import setup, find_packages


links = []  # for repo urls (dependency_links)
requires = []  # for package names

requirements = parse_requirements('requirements.txt', session=download.PipSession())

for item in requirements:
    if getattr(item, 'url', None):  # older pip has url
        links.append(str(item.url))
    if getattr(item, 'link', None):  # newer pip has link
        links.append(str(item.link))
    if item.req:
        requires.append(str(item.req))  # always the package name

setup(name='wasprng',
      version='0.1',
      description='Wasp release note generator',
      url='https://gitlab.com/AgileFactory/Agile.Wasp2',
      author='Andrea Fonti',
      author_email='andrea.fonti@agilelab.it',
      license='MIT',
      packages=['wasprng'],
      install_requires=requires,
      dependency_links=links,
      entry_points = {
        'console_scripts': ['wasp-release-note-generator=wasprng.cli:main'],
      },
      zip_safe=False)
