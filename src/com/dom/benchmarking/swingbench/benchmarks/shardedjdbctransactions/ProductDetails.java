package com.dom.benchmarking.swingbench.benchmarks.shardedjdbctransactions;


public class ProductDetails {

    double price;
    int    productID;
    int    quantityAvailable;
    int    warehouseID;

    public double getPrice() {
        return price;
    }

    public int getProductID() {
        return productID;
    }

    public int getQuantityAvailable() {
        return quantityAvailable;
    }

    public int getWarehouseID() {
        return warehouseID;
    }


    public ProductDetails(int pid, int wid, int qo) {
        productID = pid;
        warehouseID = wid;
        quantityAvailable = qo;
    }

    public ProductDetails(int pid) {
        productID = pid;
        warehouseID = 999;
        quantityAvailable = 99999;
    }

}
