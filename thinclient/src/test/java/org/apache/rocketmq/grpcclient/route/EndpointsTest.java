package org.apache.rocketmq.grpcclient.route;

import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

public class EndpointsTest {
    @Test
    public void testEndpointsWithSingleIpv4() {
        final Endpoints endpoints = new Endpoints("127.0.0.1:8080");
        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals(1, endpoints.getAddresses().size());
        final Address address = endpoints.getAddresses().iterator().next();
        Assert.assertEquals("127.0.0.1", address.getHost());
        Assert.assertEquals(8080, address.getPort());

        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals("ipv4:127.0.0.1:8080", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithMultipleIpv4() {
        final Endpoints endpoints = new Endpoints("127.0.0.1:8080;127.0.0.2:8081");
        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals(2, endpoints.getAddresses().size());
        final Iterator<Address> iterator = endpoints.getAddresses().iterator();

        final Address address0 = iterator.next();
        Assert.assertEquals("127.0.0.1", address0.getHost());
        Assert.assertEquals(8080, address0.getPort());

        final Address address1 = iterator.next();
        Assert.assertEquals("127.0.0.2", address1.getHost());
        Assert.assertEquals(8081, address1.getPort());

        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals("ipv4:127.0.0.1:8080,127.0.0.2:8081", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithSingleIpv6() {
        final Endpoints endpoints = new Endpoints("1050:0000:0000:0000:0005:0600:300c:326b:8080");
        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals(1, endpoints.getAddresses().size());
        final Address address = endpoints.getAddresses().iterator().next();
        Assert.assertEquals("1050:0000:0000:0000:0005:0600:300c:326b", address.getHost());
        Assert.assertEquals(8080, address.getPort());

        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals("ipv6:1050:0000:0000:0000:0005:0600:300c:326b:8080", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithMultipleIpv6() {
        final Endpoints endpoints = new Endpoints("1050:0000:0000:0000:0005:0600:300c:326b:8080;1050:0000:0000:0000:0005:0600:300c:326c:8081");
        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals(2, endpoints.getAddresses().size());
        final Iterator<Address> iterator = endpoints.getAddresses().iterator();

        final Address address0 = iterator.next();
        Assert.assertEquals("1050:0000:0000:0000:0005:0600:300c:326b", address0.getHost());
        Assert.assertEquals(8080, address0.getPort());

        final Address address1 = iterator.next();
        Assert.assertEquals("1050:0000:0000:0000:0005:0600:300c:326c", address1.getHost());
        Assert.assertEquals(8081, address1.getPort());

        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals("ipv6:1050:0000:0000:0000:0005:0600:300c:326b:8080,1050:0000:0000:0000:0005:0600:300c:326c:8081", endpoints.getFacade());
    }
}