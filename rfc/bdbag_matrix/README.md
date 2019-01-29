### Running matrix2bag.ipynb

Run this notebook in the `azul` environment, and preferrably use your personal environment as data are written to an AWS S3 bucket. Before launching it create a virtual environment `ENVNAME` for Python 3, activate it, install the requirements, and run
```bash
python -m ipykernel install --user --name ENVNAME --display-name "<choose some name displayed in the notebook>"
jupyter notebook matrix2bag.ipynb
```
That should open the notebook and you should see the name of environment in the upper right corner.
