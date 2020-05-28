from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()


version = '0.4.0'

install_requires = [
    'Pydap'
]


setup(name='pydap.handlers.csv',
    version=version,
    description="A handler that allows Pydap to server CSV files.",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3"
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
    entry_points="""
        [pydap.handler]    
        csv = pydap.handlers.csv:CSVHandler
    """,
)
