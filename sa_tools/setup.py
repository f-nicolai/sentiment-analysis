from pathlib import Path

from setuptools import find_packages, setup

if __name__ == "__main__":
    package_path = Path(__file__).parent

    with open(package_path / f"requirements.txt") as fp:
        install_requires = fp.readlines()

    setup(
        name='sa-tools',
        version='0.1',
        author='Florian Nicolaï',
        python_requires=">=3.9",
        classifiers=[
            "Programming Language :: Python :: 3.9",
        ],
        packages=find_packages(),
        install_requires=install_requires,
        include_package_data=True,
        zip_safe=False,
    )

