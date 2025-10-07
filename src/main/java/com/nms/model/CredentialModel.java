package com.nms.model;

public record CredentialModel(

        String credentialProfileName,

        String protocol,

        String userName,

        String password

) {}
