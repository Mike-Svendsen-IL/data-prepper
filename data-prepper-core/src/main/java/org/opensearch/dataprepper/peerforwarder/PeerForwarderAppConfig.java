/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PeerForwarderAppConfig {

    @Bean
    public PeerForwarderConfiguration peerForwarderConfiguration(
            @Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getPeerForwarderConfiguration() != null) {
                return dataPrepperConfiguration.getPeerForwarderConfiguration();
            }
        else
            return new PeerForwarderConfiguration();
    }

    @Bean
    public PeerClientPool peerClientPool() {
        return new PeerClientPool();
    }

    @Bean
    public PeerForwarderClientFactory peerForwarderClientFactory(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerClientPool peerClientPool
    ) {
        return new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool);
    }

}