package com.egs.entity;
public class Movie {
    private String movieCode;
    private int movieId;

    public Movie(String movieCode, int movieId) {
        this.movieCode = movieCode;
        this.movieId = movieId;
    }

    public String getMovieCode() {
        return movieCode;
    }

    public void setMovieCode(String movieCode) {
        this.movieCode = movieCode;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }
}
