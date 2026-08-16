package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.Binding;

import java.io.Serializable;

public class BindingDeleteArguments extends BasicArguments implements Serializable {
    private Binding binding;

    public Binding getBinding() {
        return binding;
    }

    public void setBinding(Binding binding) {
        this.binding = binding;
    }
}
