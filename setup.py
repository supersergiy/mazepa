import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mazepa",
    version="0.1.4",
    author="Sergiy Popovych",
    author_email="sergiy.popovich@gmail.com",
    description="A tool for efficient scheduling of independent tasks to remote workers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/supersergiy/mazepa",
    include_package_data=True,
    package_data={'': ['*.py']},
    install_requires=[
      'boto3',
      'click',
      'task-queue',
      'tenacity'
    ],
    packages=setuptools.find_packages(),
)
