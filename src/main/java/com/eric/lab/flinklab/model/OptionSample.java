package com.eric.lab.flinklab.model;


import java.util.Optional;

public class OptionSample {
    public static void main(String[] args) {
        // ofNullable demo
        User emptyUser = null;
        User existUser = new User("tom", "tom@126.com");
        Optional<User> opt = Optional.ofNullable(emptyUser);
        //通过ifPresent来确保在u不为空的时候执行print操作
        opt.ifPresent(u -> System.out.println(u.getName()));
        //通过orElse来获取对象为空时候的默认值,无论opt里的对象是否为空，createDefaultUser都会执行
        System.out.println(opt.orElse(createDefaultUser()).getName());
        //通过orElseGet来获取对象为空时候的默认值,只有opt为空，createDefaultUser才会执行
        System.out.println(opt.orElseGet(() -> createDefaultUser()).getName());
        // map操作
        System.out.println(opt.map(u -> u.getEmail()).orElse("default@126.com"));
        // flatMap操作,返回值为Optional<U>
        System.out.println(opt.flatMap(u -> u.getOptName()).orElse("defaultName"));
        // 基于Optional对象的Optional 类的链结构
//        System.out.println(opt.flatMap(User::getAddress).
//                flatMap(Address::getCountry).
//                map(c -> c.getCode()).orElse("defaultcode"));
        // 基于普通对象的Optional 类的链结构
        System.out.println(opt.map(u -> u.address).
                map(a -> a.country).
                map(c -> c.getCode()).orElse("defaultcode"));
    }

    private static User createDefaultUser() {
        System.out.println("create default user..........");
        User defaultUser = new User("default", "default@126.com");
        return defaultUser;
    }
}

class Country {
    public String code;

    public Country(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}

class Address {
    public Country country;

    public Address(Country country) {
        this.country = country;
    }

    public Optional<Country> getCountry() {
        return Optional.ofNullable(country);
    }

}


class User {
    private String name;
    private String email;


    public Address address;

    public User() {
    }

    public Optional<Address> getAddress() {
        return Optional.ofNullable(address);
    }

    public void setAddress(Address address) {
        this.address = address;
    }


    public User(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public Optional<String> getOptName() {
        return Optional.ofNullable(name);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
