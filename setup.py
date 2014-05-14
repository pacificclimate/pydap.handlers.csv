from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()


version = '0.3'

install_requires = [
    'Pydap >=3.2.1'
]


setup(name='pydap.handlers.csv',
    version=version,
    description="A handler that allows Pydap to server CSV files.",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords='csv opendap pydap dap data access',
    author='Roberto De Almeida',
    author_email='roberto@dealmeida.net',
    url='http://pydap.org/handlers.html#csv',
    license='MIT',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    namespace_packages = ['pydap', 'pydap.handlers'],
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    dependency_links = ['https://github.com/pacificclimate/pydap-pdp/tarball/master#egg=Pydap-3.2.2'],
    entry_points="""
        [pydap.handler]    
        csv = pydap.handlers.csv:CSVHandler
    """,
)
