package com.dk.parser;

public class SequenceGenerator {

	private static int seq = 0;
	public static int getNextSequence(){
		return ++seq;
	}
}
