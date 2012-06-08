package com.yahoo.glimmer.web;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.yahoo.glimmer.query.Context;
import com.yahoo.glimmer.query.RDFIndex;

public class IndexMap extends HashMap<String, RDFIndex> {
    private static final long serialVersionUID = -2657141430471199765L;

    private static IndexMap instance;

    @Deprecated
    public static IndexMap getInstance() {
	if (instance == null) {
	    throw new IllegalStateException("IndexMap not initialised!");
	}
	return instance;
    }

    private String configFilename;

    public String getConfigFilename() {
	return configFilename;
    }

    public void setConfigFilename(String configFilename) {
	this.configFilename = configFilename;
    }

    @PostConstruct
    public void load() throws IOException {
	Context context = new Context(configFilename);

	if (context.getMultiIndexPath() == null) {
	    // Single index, index.path property must be present
	    RDFIndex index = new RDFIndex(context);
	    put(context.getPathToIndex(), index);
	} else {
	    // Multiple indices under a root directory
	    // In this case the config file is a template that we configure for
	    // each index
	    for (File file : new File(context.getMultiIndexPath()).listFiles()) {
		if (file.isDirectory() && file.getName().matches("nq2index\\.\\w+")) {
		    String indexName = file.getName().substring("nq2index.".length());
		    Context contextCopy = new Context(context);
		    contextCopy.setIndexPath(file.getAbsolutePath());
		    RDFIndex index = new RDFIndex(contextCopy);
		    put(indexName, index);
		}
	    }
	}
	instance = this;
    }

    @PreDestroy
    protected void unload() throws Throwable {
	for (RDFIndex index : values()) {
	    if (index != null) {
		index.destroy();
	    }
	}
    }
}
