from setuptools import setup, find_packages

requires = [
  'psutil',
  'pytz',
  'databricks-connect==5.5.3',
  'six==1.13.0',
  'xmltodict==0.12.0'
]

version = {}
with open('./ETL/__version__.py') as fp:
    exec(fp.read(), version)

version = version['__version__']

with open('readme.md', 'r', encoding='utf-8') as f:
  readme = f.read()

setup(
  name='ETL_lib',
  version=version,
  packages=find_packages(),
  url='https://git.bnc.ca/users/parj010/repos/etl/browse',
  license='',
  author='Jasmin Parent',
  author_email='jasmin.parent2987@gmail.com',
  description='',
  long_description=readme,
  include_package_data=True,
  long_description_content_type='text/markdown',
  python_requires=">=3.5.*",
  install_requires=requires,
  zip_safe=False
)
