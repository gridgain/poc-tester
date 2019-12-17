/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.scenario.internal;

import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TaskProperties extends Properties {
    /** */
    private static final Logger LOG = LogManager.getLogger(TaskProperties.class.getName());
    /** */
    public static final String TASK_NAME_PROPERTY = "MAIN_CLASS";

    /** */
    public static final String TASK_THREADS_PROPERTY = "threads";

    /** */
    public static final String PARAMETER_TIME_TO_WORK = "timeToWork";

    /** */
    public static final String PARAMETER_REPORT_INTERVAL = "reportInterval";

    /**
     * Gets property with given name.
     *
     * @param name Name of the property.
     * @return Integer value of the property.
     *         Returns {@code null} value if property with given name is found.
     */
    @Nullable public String getString(String name) {
        assert name != null;

        String v = getProperty(name);

        if (v == null)
            v = System.getenv(name);

        return v;
    }

    /**
     * Gets property with given name.
     *
     * @param name Name of the property.
     * @param dflt Default value
     * @return String value of the property.
     *         Returns default value if property with given name is found.
     */
    @Nullable public String getString(String name, String dflt) {
        String val = getString(name);

        return val == null ? dflt : val;
    }

    /**
     * Gets property with given name.
     * The result is transformed to {@code boolean} using {@code Boolean.valueOf()} method.
     *
     * @param name Name of the property.
     * @param dflt Default value
     * @return Boolean value of the property.
     *         Returns default value if property with given name is found.
     */
    public boolean getBoolean(String name, boolean dflt) {
        String val = getString(name);

        return val == null ? dflt : Boolean.valueOf(val);
    }

    /**
     * Gets property with given name.
     * The result is transformed to {@code int} using {@code Integer.parseInt()} method.
     *
     * @param name Name of the property.
     * @param dflt Default value
     * @return Integer value of the property.
     *         Returns default value if property with given name is found.
     */
    public int getInteger(String name, int dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        int res;

        try {
            res = Integer.parseInt(s);
        }
        catch (NumberFormatException e) {
            LOG.error("Number format exception error message: " + e.getMessage());

            res = dflt;
        }

        return res;
    }

    /**
     * Gets property with given name.
     * The result is transformed to {@code float} using {@code Float.parseFloat()} method.
     *
     * @param name Name of the property.
     * @param dflt Default value
     * @return Float value of the property.
     *         Returns default value if property with given name is found.
     */
    public float getFloat(String name, float dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        float res;

        try {
            res = Float.parseFloat(s);
        }
        catch (NumberFormatException e) {
            LOG.error("Number format exception error message: " + e.getMessage());

            res = dflt;
        }

        return res;
    }

    /**
     * Gets property with given name.
     * The result is transformed to {@code long} using {@code Long.parseLong()} method.
     *
     * @param name Name of the property.
     * @param dflt Default value
     * @return Long value of the property.
     *         Returns default value if property with given name is found.
     */
    public long getLong(String name, long dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        long res;

        try {
            res = Long.parseLong(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets property with given name.
     * The result is transformed to {@code double} using {@code Double.parseDouble()} method.
     *
     * @param name Name of the property.
     * @param dflt Default value
     * @return Double value of the property.
     *         Returns default value if property with given name is found.
     */
    public double getDouble(String name, double dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        double res;

        try {
            res = Double.parseDouble(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }
}
