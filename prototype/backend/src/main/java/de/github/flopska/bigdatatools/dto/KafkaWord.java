package de.github.flopska.bigdatatools.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class KafkaWord {
	private String entered_word;
	private String expected_word;
	private long start_time;
	private long end_time;
}
