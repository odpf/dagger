package com.gojek.daggers.postProcessors.external.common;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;

public class DescriptorManager {
    private StencilClient stencilClient;

    public DescriptorManager(StencilClientOrchestrator stencilClientOrchestrator) {
        stencilClient = stencilClientOrchestrator.getStencilClient();
    }

    public Descriptors.Descriptor getDescriptor(String protoClassName) {
        Descriptors.Descriptor descriptor = stencilClient.get(protoClassName);
        if (descriptor == null) {
            throw new DescriptorNotFoundException("No Descriptor found for class "
                    + protoClassName);
        }
        return descriptor;
    }
}
