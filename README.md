# Apple Store Sales Data Analysis
This is a data project to analyze the Apple Store's sales data. The project is based on the [datamade/apple-store-saless](https://github.com/datamade/apple-store-saless) repository.

The goal of this project is to analyze the data and create visualizations to help understand the trends and patterns in the sales data while answering questions such as: 
- What customers purchased Airpods after purchasing the iPhone.
- What customers purchased Airpods and the iPhone.
- List all products purchased by customers after their initial purchase.

## Data Source
The data is from the [Apple Store](https://www.apple.com/shop/buy-iphone/) and is available for download at [https://www.apple.com/shop/buy-iphone/](https://www.apple.com/shop/buy-iphone/).

## Data Processing
The data is processed using the [Pyspark](https://spark.apache.org/docs/latest/index.html) library. The data is loaded into a Spark DataFrame and then processed using a series of transformations. The final output is a DataFrame containing the customer data, transaction data, and product data.

## Data Visualization
The data is visualized using a series of charts and graphs. The charts and graphs are created using the [Matplotlib](https://matplotlib.org/) library in Python.

## Data Analysis
The data is analyzed using a series of statistical techniques and algorithms. The analysis is performed using the [Scikit-learn](https://scikit-learn.org/stable/) library in Python.





## Data
The data is available at [https://github.com/datamade/apple-store-saless](https://github.com/datamade/apple-store-saless).

## Data Dictionary

### Customers

| Field | Type | Description |
| ----- | ---- | ----------- |
| `customer_id` | `int` | Unique identifier for the customer. |
| `customer_name` | `string` | Name of the customer. |
| `join_date` | `date` | Date the customer joined the store. |
| `location` | `string` | Location of the customer. |


### Transactions

| Field | Type | Description |
| ----- | ---- | ----------- |
| `transaction_id` | `int` | Unique identifier for the transaction. |
| `customer_id` | `int` | Unique identifier for the customer. |
| `product_name` | `string` | Name of the product. |
| `transaction_date` | `date` | Date of the transaction. |

### Products
| Field | Type | Description |
| ----- | ---- | ----------- |
| `product_id` | `int` | Unique identifier for the product. |
| `product_name` | `string` | Name of the product. |
| `price` | `float` | Price of the product. |
| `category` | `string` | Category of the product. |


## Installation

To install the project, you will need to have Python 3 installed on your computer. You can then use the following command to install the required packages:

```
pip install -r requirements.txt
```

Once the packages are installed, you can run the project by running the following command:

```
python main.py
```
or
```
spark-submit --master local[*] main.py
```

This will start the Spark application and process the data.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact
If you have any questions or comments, please feel free to contact me at [ofililewis@gmail.com](mailto:ofililewis@gmail.com).

## Acknowledgments
* [datamade/apple-store-saless](https://github.com/datamade/apple-store-saless)
* [pyspark](https://spark.apache.org/docs/latest/index.html)
* [matplotlib](https://matplotlib.org/)
* [scikit-learn](https://scikit-learn.org/stable/)