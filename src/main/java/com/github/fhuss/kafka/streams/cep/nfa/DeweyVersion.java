/**
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
package com.github.fhuss.kafka.streams.cep.nfa;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Class to wrap a Dewey version number.
 */
public class DeweyVersion {

    private int[] versions;

    public DeweyVersion() {}

    /**
     * Creates a new {@link DeweyVersion} instance.
     *
     * @param run the first version number.
     */
    public DeweyVersion(int run) {
        this(new int[]{run});
    }

    public DeweyVersion(String s) {
        String[] integers = s.split("\\.");
        this.versions = new int[integers.length];
        for(int i = 0; i < integers.length; i++)
            this.versions[i] = Integer.parseInt(integers[i]);
    }

    private DeweyVersion(int[] versions) {
        this.versions = versions;
    }

    public DeweyVersion addRun() {
        int[] newDeweyNumber = Arrays.copyOf(versions, versions.length);
        newDeweyNumber[versions.length - 1]++;

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
