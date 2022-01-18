package com.sample;

import com.sample.avro.Product;

public class ProductParser {

    public Product parse(String line) throws Exception {
        Product product = new Product();
        String[] items = line.split(";");

        if(items.length != 3)
            throw new Exception("Error when reading line products : " + line );

        product.setProductId(items[0]);
        product.setTitle(items[1]);
        product.setPrice(Float.parseFloat(items[2]));

        return product;
    }
}
