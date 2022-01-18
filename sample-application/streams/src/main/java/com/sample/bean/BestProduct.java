package com.sample.bean;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import javax.imageio.stream.IIOByteBuffer;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;

public class BestProduct {

    public static class BestProducSerdes implements Serde<BestProduct>{

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            Serde.super.configure(configs, isKey);
        }

        @Override
        public void close() {
            Serde.super.close();
        }

        @Override
        public Serializer<BestProduct> serializer() {
            return (topic, data) ->
                 ByteBuffer
                        .allocate(data.getProductId().length() + 4)
                        .putFloat(data.getTotalSale())
                         .position(4)
                        .put(data.getProductId().getBytes())
                        .array();
        }

        @Override
        public Deserializer<BestProduct> deserializer() {
            return (topic, data) -> {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                float productSale = buffer.getFloat();
                byte[] productIdBytes = new byte[data.length - 4];
                buffer.get(productIdBytes);
                return new BestProduct(new String(productIdBytes), productSale);
            };
        }
    }

    public static class BestProductComparator implements Comparator<BestProduct> {

        @Override
        public int compare(BestProduct o1, BestProduct o2) {
            return o1.getTotalSale().compareTo(o2.getTotalSale());
        }
    }

    public BestProduct(String productId, Float totalSale) {
        this.productId = productId;
        this.totalSale = totalSale;
    }

    public BestProduct() {
    }

    private String productId;
    private Float totalSale;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Float getTotalSale() {
        return totalSale;
    }

    public void setTotalSale(Float totalSale) {
        this.totalSale = totalSale;
    }

    public static BestProducSerdes Serdes() { return new BestProducSerdes(); }
}
