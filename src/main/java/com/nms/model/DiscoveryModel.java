package com.nms.model;

import java.util.List;

public record DiscoveryModel (

        String discoveryName,

        List<String> credentialProfiles,

        String hostIP,

        int port
) {
    public DiscoveryModel {

        credentialProfiles = List.copyOf(credentialProfiles);

    }
}
