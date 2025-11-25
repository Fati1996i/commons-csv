/*

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at
https://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License. */
package org.apache.commons.csv;

import static org.apache.commons.csv.Token.Type.TOKEN;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.build.AbstractStreamBuilder;
import org.apache.commons.io.function.Uncheck;

/**

Parses CSV files according to the specified format.

Because CSV appears in many different dialects, the parser supports many formats by allowing the

specification of a {@link CSVFormat}.

The parser works record-wise. It is not possible to go back, once a record has been parsed from the input stream.

<h2>Creating instances</h2>
<p>
There are several static factory methods that can be used to create instances for various types of resources:

</p>
<ul>
<li>{@link #parse(java.io.File, Charset, CSVFormat)}</li>
<li>{@link #parse(String, CSVFormat)}</li>
<li>{@link #parse(java.net.URL, java.nio.charset.Charset, CSVFormat)}</li>
</ul>
<p>
Alternatively parsers can also be created by passing a {@link Reader} directly to the sole constructor.

For those who like fluent APIs, parsers can be created using {@link CSVFormat#parse(java.io.Reader)} as a shortcut:

</p>
<pre>
for (CSVRecord record : CSVFormat.EXCEL.parse(in)) {

...
}

</pre>
<h2>Parsing record wise</h2>
<p>
To parse a CSV input from a file, you write:

</p>
<pre>{@code
File csvData = new File("/path/to/csv");

CSVParser parser = CSVParser.parse(csvData, CSVFormat.RFC4180);

for (CSVRecord csvRecord : parser) {

...
}}

</pre>
<p>
This will read the parse the contents of the file using the

<a href="https://tools.ietf.org/html/rfc4180" target="_blank">RFC 4180</a> format.

</p>
<p>
To parse CSV input in a format like Excel, you write:

</p>
<pre>
CSVParser parser = CSVParser.parse(csvData, CSVFormat.EXCEL);

for (CSVRecord csvRecord : parser) {

...
}

</pre>
<p>
If the predefined formats don't match the format at hand, custom formats can be defined. More information about

customizing CSVFormats is available in {@link CSVFormat CSVFormat Javadoc}.

</p>
<h2>Parsing into memory</h2>
<p>
If parsing record-wise is not desired, the contents of the input can be read completely into memory.

</p>
<pre>{@code
Reader in = new StringReader("a;b\nc;d");

CSVParser parser = new CSVParser(in, CSVFormat.EXCEL);

List<CSVRecord> list = parser.getRecords();

}</pre>

<p>
There are two constraints that have to be kept in mind:

</p>
<ol>
<li>Parsing into memory starts at the current position of the parser. If you have already parsed records from
the input, those records will not end up in the in-memory representation of your CSV data.</li>
<li>Parsing into memory may consume a lot of system resources depending on the input. For example, if you're
parsing a 150MB file of CSV data the contents will be read completely into memory.</li>
</ol>
<h2>Notes</h2>
<p>
The internal parser state is completely covered by the format and the reader state.

</p>
@see <a href="package-summary.html">package documentation for more details</a>
*/
public final class CSVParser implements Iterable<CSVRecord>, Closeable {

/**

Builds a new {@link CSVParser}.

@since 1.13.0
*/
public static class Builder extends AbstractStreamBuilder<CSVParser, Builder> {

private CSVFormat format;
private long characterOffset;
private long recordNumber = 1;
private boolean trackBytes;

/**

Constructs a new instance. */ protected Builder() { // empty }
@SuppressWarnings("resource")
@Override
public CSVParser get() throws IOException {
return new CSVParser(getReader(), format != null ? format : CSVFormat.DEFAULT, characterOffset, recordNumber, getCharset(), trackBytes);
}

/**

Sets the lexer offset when the parser does not start parsing at the beginning of the source.
@param characterOffset the lexer offset.
@return {@code this} instance. */ public Builder setCharacterOffset(final long characterOffset) { this.characterOffset = characterOffset; return asThis(); }
/**

Sets the CSV format. A copy of the given format is kept.
@param format the CSV format, {@code null} resets to {@link CSVFormat#DEFAULT}.
@return {@code this} instance. */ public Builder setFormat(final CSVFormat format) { this.format = CSVFormat.copy(format); return asThis(); }
/**

Sets the next record number to assign, defaults to {@code 1}.
@param recordNumber the next record number to assign.
@return {@code this} instance. */ public Builder setRecordNumber(final long recordNumber) { this.recordNumber = recordNumber; return asThis(); }
/**

Sets whether to enable byte tracking for the parser.
@param trackBytes {@code true} to enable byte tracking; {@code false} to disable it.
@return {@code this} instance.
@since 1.13.0 */ public Builder setTrackBytes(final boolean trackBytes) { this.trackBytes = trackBytes; return asThis(); }
}

final class CSVRecordIterator implements Iterator<CSVRecord> {
private CSVRecord current;

 /**
  * Gets the next record or null at the end of stream or max rows read.
  *
  * @throws IOException  on parse error or input read-failure
  * @throws CSVException on invalid input.
  * @return the next record, or {@code null} if the end of the stream has been reached.
  */
 private CSVRecord getNextRecord() {
     CSVRecord record = null;
     if (format.useRow(recordNumber + 1)) {
         record = Uncheck.get(CSVParser.this::nextRecord);
     }
     return record;
 }
