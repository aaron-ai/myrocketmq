package org.apache.rocketmq.grpcclient.route;

public enum AddressScheme {
    /**
     * Scheme for domain name.
     */
    DOMAIN_NAME(""),
    /**
     * Scheme for ipv4 address.
     */
    IPv4("ipv4:"),
    /**
     * Scheme for ipv6 address.
     */
    IPv6("ipv6:");

    private final String prefix;

    AddressScheme(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return this.prefix;
    }

    public apache.rocketmq.v1.AddressScheme toAddressScheme() {
        switch (this) {
            case IPv4:
                return apache.rocketmq.v1.AddressScheme.IPv4;
            case IPv6:
                return apache.rocketmq.v1.AddressScheme.IPv6;
            case DOMAIN_NAME:
            default:
                return apache.rocketmq.v1.AddressScheme.DOMAIN_NAME;
        }
    }
}
