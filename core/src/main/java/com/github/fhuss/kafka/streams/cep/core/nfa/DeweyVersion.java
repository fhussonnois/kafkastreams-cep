/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.core.nfa;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Class to wrap a Dewey version number.
 */
public class DeweyVersion {

    private static final String VERSION_RUN_SEP = "\\.";

    private int[] versions;

    public DeweyVersion() {}

    /**
     * Creates a new {@link DeweyVersion} instance.
     *
     * @param run    the first version number.
     */
    public DeweyVersion(final int run) {
        this(new int[]{run});
    }

    /**
     * Creates a new {@link DeweyVersion} from the specified string.
     *
     * @param version A string dewey version number.
     */
    public DeweyVersion(final String version) {
        String[] integers = version.split(VERSION_RUN_SEP);
        this.versions = new int[integers.length];
        for(int i = 0; i < integers.length; i++)
            this.versions[i] = Integer.parseInt(integers[i]);
    }

    private DeweyVersion(int[] versions) {
        this.versions = versions;
    }

    public DeweyVersion addRun() {
        return addRun(1);
    }

    public DeweyVersion addRun(final int offset) {
        int[] newDeweyNumber = Arrays.copyOf(versions, versions.length);
        newDeweyNumber[versions.length - offset]++;

        return new DeweyVersion(newDeweyNumber);
    }

    public int length() {
        return this.versions.length;
    }

    public boolean isCompatible(DeweyVersion that) {
        if (this.length() > that.length()) {
            // prefix case
            for (int i = 0; i < that.length(); i++)
                if (this.versions[i] != that.versions[i])
                    return false;

            return true;
        } else if (this.length() == that.length()) {
            // check init digits for equality
            int lastIndex = length() - 1;
            for (int i = 0; i < lastIndex; i++)
                if ( this.versions[i] != that.versions[i])
                    return false;

            // check that the last digit is greater or equal
            return this.versions[lastIndex] >= that.versions[lastIndex];
        } else {
            return false;
        }
    }

    public DeweyVersion addStage() {
        return new DeweyVersion(Arrays.copyOf(this.versions, versions.length + 1));
    }

    @Override
    public String toString() {
         return Arrays.stream(versions)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining("."));
    }
}
