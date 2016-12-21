from distutils.core import setup
from distutils.command import clean

if __name__ == "__main__":
    
    setup(
        name= 'wicetl',
        version= '?',
        description= 'WIC ETL',
        author= 'Luke',
        author_email= 'luke.huang@wavein.com.tw',
        packages= ['scripts', 'distutils', 'distutils.command'],
        package_dir= {'': 'scripts'}
    )
