package com.anand.redis.client;

import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.Serializable;
import java.util.Objects;

class Person implements Serializable {
    String firstName;
    String lastName;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Person{");
        sb.append("firstName='").append(firstName).append('\'');
        sb.append(", lastName='").append(lastName).append('\'');
        sb.append('}');
        return sb.toString();
    }

}

public class RedisClient {
    public static void putPersonInCache(RMap<String, Person> map) {
        final Person person1 = new Person();
        person1.setFirstName("A");
        person1.setLastName("Test");

        Object retObject = map.put("123", person1);
        System.out.println(retObject);
    }

    public static void main(String[] args) {
        final Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        config.useClusterServers().addNodeAddress();
        final RedissonClient client = Redisson.create(config);

        final RMap<String, Person> map = client.getMap("people");
        putPersonInCache(map);

        final Person person = map.get("123");
        System.out.println("==================================");

        if (Objects.nonNull(person)) {
            System.out.println(person);
        } else {
            System.out.println("person not found");
        }

        System.out.println("==================================");

        client.shutdown();

    }
}
