# Keyrock Order Book Aggregation Challenge
![Alt Text](img/TUI.png)
# Quick Start
The project supports Python version >=3.6 

### Install requirements
```bash
pip3 install requirements.txt
```

### Start Server
We can start the server instance by passing the base and quote asset of the pair we would like to stream data for:
```bash
python3 server.py --base_asset {base_asset} --quote_asset {quote_asset} --port {port}
```

### Start Client
After starting the server, we can run the sample client, which will listed to the data stream and output the order book:
```bash
python3 client.py --port {port}
```

![Alt Text](img/OB-Aggregator.gif)

# Implementation


# Exchange connectivity


# To-do
