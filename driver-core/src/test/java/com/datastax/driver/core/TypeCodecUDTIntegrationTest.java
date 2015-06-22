/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

@CassandraVersion(major = 2.1)
public class TypeCodecUDTIntegrationTest {

    private CCMBridge ccm;
    private Cluster cluster;
    private Session session;

    private List<String> schema = Lists.newArrayList(
        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE test",
        "CREATE TYPE IF NOT EXISTS \"phone\" (number text, tags set<text>)",
        "CREATE TYPE IF NOT EXISTS \"address\" (street text, zipcode int, phones list<frozen<phone>>)",
        "CREATE TABLE IF NOT EXISTS \"users\" (id uuid PRIMARY KEY, name text, address frozen<address>)"
    );

    private final String insertQuery = "INSERT INTO users (id, name, address) VALUES (?, ?, ?)";
    private final String selectQuery = "SELECT id, name, address FROM users WHERE id = ?";

    private final UUID uuid = UUID.randomUUID();

    private final Phone phone1 = new Phone("1234567", Sets.newHashSet("home", "iphone"));
    private final Phone phone2 = new Phone("2345678", Sets.newHashSet("work"));
    private final Address address = new Address("blah", 75010, Lists.newArrayList(phone1, phone2));

    private UserType addressType;
    private UserType phoneType;

    private UDTValue phone1Value;
    private UDTValue phone2Value;
    private UDTValue addressValue;


    @BeforeClass(groups = "short")
    public void setupCcm() {
        ccm = CCMBridge.create("test", 1);
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void teardown() {
        if (cluster != null)
            cluster.close();
    }

    @AfterClass(groups = "short", alwaysRun = true)
    public void teardownCcm() {
        if (ccm != null)
            ccm.remove();
    }

    @Test(groups = "short")
    public void should_handle_udts_with_default_codecs() {
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .build();
        createSession();
        setUpUserTypes();
        // simple statement
        session.execute(new SimpleStatement(insertQuery, uuid, "John Doe", addressValue));
        ResultSet rows = session.execute(new SimpleStatement(selectQuery, uuid));
        Row row = rows.one();
        assertRow(row);
        // prepared + values
        PreparedStatement ps = session.prepare(new SimpleStatement(insertQuery));
        session.execute(ps.bind(uuid, "John Doe", addressValue));
        rows = session.execute(new SimpleStatement(selectQuery, uuid));
        row = rows.one();
        assertRow(row);
        // bound with setUDTValue
        session.execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setUDTValue(2, addressValue));
        rows = session.execute(new SimpleStatement(selectQuery, uuid));
        row = rows.one();
        assertRow(row);
        // bound with setObject
        session.execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setObject(2, addressValue));
        rows = session.execute(new SimpleStatement(selectQuery, uuid));
        row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_handle_udts_with_custom_codecs() {
        CodecRegistry codecRegistry = new CodecRegistry();
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .withCodecRegistry(codecRegistry)
            .build();
        createSession();
        setUpUserTypes();
        TypeCodec<UDTValue> addressTypeCodec = codecRegistry.codecFor(addressType);
        TypeCodec<UDTValue> phoneTypeCodec = codecRegistry.codecFor(phoneType);
        codecRegistry
            .register(new AddressCodec(addressTypeCodec, Address.class))
            .register(new PhoneCodec(phoneTypeCodec, Phone.class))
        ;
        // we must use prepared statements with overriding codecs
        session.execute(session.prepare(insertQuery).bind(uuid, "John Doe", address));
        ResultSet rows = session.execute(new SimpleStatement(selectQuery, uuid));
        Row row = rows.one();
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.getObject(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1, String.class)).isEqualTo("John Doe");
        // not possible, codec deserializes to Address directly
        //assertThat(row.getUDTValue(2)).isEqualTo(address);
        assertThat(row.getObject(2)).isEqualTo(address);
        assertThat(row.getObject(2, Address.class)).isEqualTo(address);
    }

    // TODO overriding codecs
    
    private void assertRow(Row row) {
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.getObject(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getUDTValue(2)).isEqualTo(addressValue);
        assertThat(row.getObject(2)).isEqualTo(addressValue);
        assertThat(row.getObject(2, UDTValue.class)).isEqualTo(addressValue);
    }

    private void createSession() {
        cluster.init();
        session = cluster.connect();
        for (String statement : schema)
            session.execute(statement);
    }

    private void setUpUserTypes() {
        addressType = cluster.getMetadata().getKeyspace("test").getUserType("address");
        phoneType = cluster.getMetadata().getKeyspace("test").getUserType("phone");
        phone1Value = phoneType.newValue()
            .setString("number", phone1.number)
            .setSet("tags", phone1.tags);
        phone2Value = phoneType.newValue()
            .setString("number", phone2.number)
            .setObject("tags", phone2.tags);
        addressValue = addressType.newValue()
            .setString("street", address.street)
            .setInt(1, address.zipcode)
            .setList("phones", Lists.newArrayList(phone1Value, phone2Value));
    }

    static class AddressCodec extends TypeCodec.MappingCodec<Address, UDTValue> {

        private final UserType userType;

        public AddressCodec(TypeCodec<UDTValue> innerCodec, Class<Address> javaType) {
            super(innerCodec, javaType);
            userType = (UserType) innerCodec.getCqlType();
        }

        @Override
        protected Address deserialize(UDTValue value) {
            return new Address(value.getString("street"), value.getInt("zipcode"), value.getList("phones", Phone.class));
        }

        @Override
        protected UDTValue serialize(Address value) {
            return userType.newValue().setString("street", value.street).setInt("zipcode", value.zipcode).setList("phones", value.phones);
        }
    }

    static class PhoneCodec extends TypeCodec.MappingCodec<Phone, UDTValue> {

        private final UserType userType;

        public PhoneCodec(TypeCodec<UDTValue> innerCodec, Class<Phone> javaType) {
            super(innerCodec, javaType);
            userType = (UserType) innerCodec.getCqlType();
        }

        @Override
        protected Phone deserialize(UDTValue value) {
            return new Phone(value.getString("number"), value.getSet("tags", String.class));
        }

        @Override
        protected UDTValue serialize(Phone value) {
            return userType.newValue().setString("number", value.number).setSet("tags", value.tags);
        }
    }

    static class Address {

        String street;

        int zipcode;

        List<Phone> phones;

        public Address(String street, int zipcode, List<Phone> phones) {
            this.street = street;
            this.zipcode = zipcode;
            this.phones = phones;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Address address = (Address)o;

            if (zipcode != address.zipcode)
                return false;
            if (!street.equals(address.street))
                return false;
            return phones.equals(address.phones);

        }

        @Override
        public int hashCode() {
            int result = street.hashCode();
            result = 31 * result + zipcode;
            result = 31 * result + phones.hashCode();
            return result;
        }
    }

    static class Phone {

        String number;

        Set<String> tags;

        public Phone(String number, Set<String> tags) {
            this.number = number;
            this.tags = tags;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Phone phone = (Phone)o;

            if (!number.equals(phone.number))
                return false;
            return tags.equals(phone.tags);

        }

        @Override
        public int hashCode() {
            int result = number.hashCode();
            result = 31 * result + tags.hashCode();
            return result;
        }
    }
}
