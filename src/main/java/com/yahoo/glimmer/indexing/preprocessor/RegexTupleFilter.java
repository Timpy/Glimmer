package com.yahoo.glimmer.indexing.preprocessor;

/*
 * Copyright (c) 2012 Yahoo! Inc. All rights reserved.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is 
 *  distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *  See accompanying LICENSE file.
 */

import java.util.regex.Pattern;

/**
 * Simple filter that applies regex matches to each element.
 * 
 * @author tep
 */
public class RegexTupleFilter implements TupleFilter {
    private Pattern subjectPattern;
    private Pattern predicatePattern;
    private Pattern objectPattern;
    private Pattern contextPattern;
    private boolean andNotOrConjunction;

    public Pattern getSubjectPattern() {
	return subjectPattern;
    }
    public void setSubjectRegex(String regex) {
        this.subjectPattern = Pattern.compile(regex);
    }

    public Pattern getPredicatePattern() {
	return predicatePattern;
    }
    public void setPredicateRegex(String regex) {
        this.predicatePattern = Pattern.compile(regex);
    }

    public Pattern getObjectPattern() {
	return objectPattern;
    }
    public void setObjectRegex(String regex) {
        this.objectPattern = Pattern.compile(regex);
    }

    public Pattern getContextPattern() {
	return contextPattern;
    }
    public void setContextRegex(String regex) {
        this.contextPattern = Pattern.compile(regex);
    }

    public boolean isAndNotOrConjunction() {
	return andNotOrConjunction;
    }
    public void setAndNotOrConjunction(boolean andNotOrConjunction) {
        this.andNotOrConjunction = andNotOrConjunction;
    }

    @Override
    public boolean filter(Tuple tuple) {
	int tried = 0;
	int matched = 0;
	
	if (subjectPattern != null) {
	    tried++;
	    if (subjectPattern.matcher(tuple.subject.n3).find()) {
		if (!andNotOrConjunction) {
		    return true;
		}
		matched++;
	    }
	}
	if (predicatePattern != null) {
	    tried++;
	    if (predicatePattern.matcher(tuple.predicate.n3).find()) {
		if (!andNotOrConjunction) {
		    return true;
		}
		matched++;
	    }
	}
	if (objectPattern != null) {
	    tried++;
	    if (objectPattern.matcher(tuple.object.n3).find()) {
		if (!andNotOrConjunction) {
		    return true;
		}
		matched++;
	    }
	}
	if (contextPattern != null && tuple.context.n3 != null) {
	    tried++;
	    if (contextPattern.matcher(tuple.context.n3).find()) {
		if (!andNotOrConjunction) {
		    return true;
		}
		matched++;
	    }
	}

	// AND. tried should equal matched
	if (tried == matched) {
	    return true;
	}
	return false;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(RegexTupleFilter.class.getSimpleName());
        sb.append('\n');
        if (subjectPattern != null) {
            sb.append("\tSubject:");
            sb.append(subjectPattern.toString());
            sb.append('\n');
        }
        if (predicatePattern != null) {
            sb.append("\tPredicate:");
            sb.append(predicatePattern.toString());
            sb.append('\n');
        }
        if (objectPattern != null) {
            sb.append("\tObject:");
            sb.append(objectPattern.toString());
            sb.append('\n');
        }
        if (contextPattern != null) {
            sb.append("\tContext:");
            sb.append(contextPattern.toString());
            sb.append('\n');
        }
        sb.append("\tConjunction:");
        if (andNotOrConjunction) {
            sb.append("AND");
        } else {
            sb.append("OR");
        }
        sb.append('\n');
        return sb.toString();
    }
}
