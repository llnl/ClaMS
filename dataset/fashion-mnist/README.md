
To download the Fashion MNIST dataset, 
run the following commands:

```shell

# Skip this step if you are already in a virtual environment
python3 -m venv ./venv
source ./venv/bin/activate

git clone git@github.com:zalandoresearch/fashion-mnist.git
pip install -r fashion-mnist/requirements.txt
PYTHONPATH=fashion-mnist/utils python3 dump_data.py
```