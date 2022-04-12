/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.grpcclient.route;

import com.google.common.base.Objects;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Endpoints {
    private static final String ADDRESS_SEPARATOR = ",";
    private final AddressScheme scheme;

    /**
     * URI path for grpc target, e.g:
     * <p>1. domain name: dns:rocketmq.apache.org:8080
     * <p>2. ipv4:127.0.0.1:10911[,127.0.0.2:10912]
     * <p>3. ipv6:1050:0000:0000:0000:0005:0600:300c:326b:10911[,1050:0000:0000:0000:0005:0600:300c:326b:10912]
     */
    private final String facade;
    private final List<Address> addresses;

    public Endpoints(apache.rocketmq.v2.Endpoints endpoints) {
        this.addresses = new ArrayList<>();
        for (apache.rocketmq.v2.Address address : endpoints.getAddressesList()) {
            addresses.add(new Address(address));
        }
        if (addresses.isEmpty()) {
            throw new UnsupportedOperationException("No available address");
        }

        final apache.rocketmq.v2.AddressScheme scheme = endpoints.getScheme();

        switch (scheme) {
            case IPv4:
                this.scheme = AddressScheme.IPv4;
                break;
            case IPv6:
                this.scheme = AddressScheme.IPv6;
                break;
            case DOMAIN_NAME:
            default:
                this.scheme = AddressScheme.DOMAIN_NAME;
                if (addresses.size() > 1) {
                    throw new UnsupportedOperationException("Multiple addresses not allowed in domain schema");
                }
        }
        StringBuilder facadeBuilder = new StringBuilder();
        facadeBuilder.append(this.scheme.getPrefix());
        for (Address address : addresses) {
            facadeBuilder.append(address.getAddress()).append(ADDRESS_SEPARATOR);
        }
        this.facade = facadeBuilder.substring(0, facadeBuilder.length() - 1);
    }

    public Endpoints(String facade) {
        this.facade = facade;
        final int prefixIndex = facade.indexOf(":");
        final String prefix = facade.substring(0, prefixIndex + 1);
        final String suffix = facade.substring(1 + prefixIndex);
        this.addresses = new ArrayList<>();
        final AddressScheme scheme = AddressScheme.fromPrefix(prefix);
        this.scheme = scheme;
        switch (scheme) {
            case DOMAIN_NAME: {
                final String[] split = suffix.split(":");
                String host = split[0];
                int port = split.length >= 2 ? Integer.parseInt(split[1]) : 80;
                final Address address = new Address(host, port);
                addresses.add(address);
                break;
            }
            case IPv4:
            case IPv6: {
                final String[] split = suffix.split(ADDRESS_SEPARATOR);
                for (String str : split) {
                    final int portIndex = str.lastIndexOf(":");
                    String host = str.substring(0, portIndex);
                    int port = Integer.parseInt(str.substring(1 + portIndex));
                    final Address address = new Address(host, port);
                    addresses.add(address);
                }
            }
        }
    }

    public Endpoints(AddressScheme scheme, List<Address> addresses) {
        if (AddressScheme.DOMAIN_NAME.equals(scheme) && addresses.size() > 1) {
            throw new UnsupportedOperationException("Multiple addresses not allowed in domain schema.");
        }
        checkNotNull(addresses, "addresses");
        if (addresses.isEmpty()) {
            throw new UnsupportedOperationException("No available address");
        }

        this.scheme = scheme;
        this.addresses = addresses;
        StringBuilder facadeBuilder = new StringBuilder();
        facadeBuilder.append(scheme.getPrefix());
        for (Address address : addresses) {
            facadeBuilder.append(address.getAddress()).append(ADDRESS_SEPARATOR);
        }
        this.facade = facadeBuilder.substring(0, facadeBuilder.length() - 1);
    }

    public AddressScheme getScheme() {
        return scheme;
    }

    public List<Address> getAddresses() {
        return addresses;
    }

    public List<InetSocketAddress> toSocketAddresses() {
        switch (scheme) {
            case DOMAIN_NAME:
                return null;
            case IPv4:
            case IPv6:
            default:
                // Customize the name resolver to support multiple addresses.
                List<InetSocketAddress> socketAddresses = new ArrayList<InetSocketAddress>();
                for (Address address : addresses) {
                    socketAddresses.add(new InetSocketAddress(address.getHost(), address.getPort()));
                }
                return socketAddresses;
        }
    }

    public apache.rocketmq.v2.Endpoints toProtobuf() {
        final apache.rocketmq.v2.Endpoints.Builder builder = apache.rocketmq.v2.Endpoints.newBuilder();
        for (Address address : addresses) {
            builder.addAddresses(address.toProtobuf());
        }
        return builder.setScheme(scheme.toProtobuf()).build();
    }

    @Override
    public String toString() {
        return facade;
    }

    public String getFacade() {
        return this.facade;
    }

    public String getGrpcTarget() {
        if (AddressScheme.DOMAIN_NAME.equals(scheme)) {
            return facade.substring(scheme.getPrefix().length());
        }
        return facade;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Endpoints endpoints = (Endpoints) o;
        return scheme == endpoints.scheme && Objects.equal(facade, endpoints.facade) &&
            Objects.equal(addresses, endpoints.addresses);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(scheme, facade, addresses);
    }
}
