package com.sample.bean;

import com.sample.avro.Command;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class CommandJsonModel {

    public static class ProductCommandJsonModel {
        private String productId;
        private int quantity;

        public ProductCommandJsonModel(String productId, int quantity) {
            this.productId = productId;
            this.quantity = quantity;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
    }

    public static class ProductJsonModel {
        private java.lang.String productId;
        private java.lang.String title;
        private float price;

        public ProductJsonModel(String productId, String title, float price) {
            this.productId = productId;
            this.title = title;
            this.price = price;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public float getPrice() {
            return price;
        }

        public void setPrice(float price) {
            this.price = price;
        }
    }

    private String commandId;
    private Instant commandDate;
    private float total;
    private String user;
    private List<ProductCommandJsonModel> items;
    private List<ProductJsonModel> products;

    public String getCommandId() {
        return commandId;
    }

    public void setCommandId(String commandId) {
        this.commandId = commandId;
    }

    public Instant getCommandDate() {
        return commandDate;
    }

    public void setCommandDate(Instant commandDate) {
        this.commandDate = commandDate;
    }

    public float getTotal() {
        return total;
    }

    public void setTotal(float total) {
        this.total = total;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<ProductCommandJsonModel> getItems() {
        return items;
    }

    public void setItems(List<ProductCommandJsonModel> items) {
        this.items = items;
    }

    public List<ProductJsonModel> getProducts() {
        return products;
    }

    public void setProducts(List<ProductJsonModel> products) {
        this.products = products;
    }

    public static CommandJsonModel to(Command command){
        CommandJsonModel json = new CommandJsonModel();
        json.setCommandDate(command.getCommandDate());
        json.setCommandId(command.getCommandId());
        json.setTotal(command.getTotal());
        json.setUser(command.getUser());
        json.setItems(command.getItems().stream().map(i -> new ProductCommandJsonModel(i.getProductId(), i.getQuantity())).collect(Collectors.toList()));
        json.setProducts(command.getProducts().stream().map(p -> new ProductJsonModel(p.getProductId(), p.getTitle(), p.getPrice())).collect(Collectors.toList()));
        return json;
    }
}