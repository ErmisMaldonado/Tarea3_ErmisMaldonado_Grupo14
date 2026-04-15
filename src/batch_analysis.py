from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

def main():
    # Crear sesión de Spark
    spark = SparkSession.builder.appName("EcommerceBatch").getOrCreate()

    # Ruta del dataset (ajústala si es necesario)
    path = "data/ecommerce_customer_churn_dataset.csv"

    # Cargar datos
    df = spark.read.csv(path, header=True, inferSchema=True)

    print("\n=== Muestra de datos ===")
    df.show(5)

    print("\n=== Esquema ===")
    df.printSchema()

    # Limpieza
    df = df.dropna()

    # =========================
    # ANÁLISIS (EDA)
    # =========================

    print("\n=== Conteo de clientes por churn ===")
    df.groupBy("Churned").count().show()

    print("\n=== Promedio de compras ===")
    df.select(avg("Total_Purchases").alias("avg_purchases")).show()

    print("\n=== Promedio de compras por país ===")
    df.groupBy("Country").avg("Total_Purchases").show()

    print("\n=== Promedio valor de compra por género ===")
    df.groupBy("Gender").avg("Average_Order_Value").show()

    print("\n=== Clientes por país ===")
    df.groupBy("Country").count().show()

    # =========================
    # VISUALIZACIÓN
    # =========================
    try:
        import matplotlib.pyplot as plt

        pdf = df.groupBy("Country").count().toPandas()
        pdf.plot(kind="bar", x="Country", y="count", legend=False)
        plt.title("Clientes por país")
        plt.xlabel("País")
        plt.ylabel("Cantidad de clientes")
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print("No se pudo generar el gráfico:", e)

    spark.stop()

if __name__ == "__main__":
    main()
