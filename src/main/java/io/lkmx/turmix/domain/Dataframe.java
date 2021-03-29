package io.lkmx.turmix.domain;

public interface Dataframe {

    DataDescriptor UNDEFINED = () -> new String[]{};

    String name();

    DataDescriptor descriptor();

}
