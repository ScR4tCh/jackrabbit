from distutils.core import setup

setup(
  name = 'jackrabbit',
  packages = ['jackrabbit'],
  version = '0.1',
  description = 'Pika based, decorated threaded amqp consumers',
  author = 'Stefan Reiser',
  author_email = 'mailto.stefan@gmx.de',
  url = 'https://github.com/ScR4tCh/jackrabbit',
  download_url = 'https://github.com/ScR4tCh/jackrabbit/archive/0.1.tar.gz',
  keywords = ['rabbitmq', 'messagequeue', 'amqp', 'consumer'],
  classifiers = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
  ],
  install_requires=['pika']
)
