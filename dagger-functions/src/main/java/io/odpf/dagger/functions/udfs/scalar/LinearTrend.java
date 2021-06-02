package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LinearTrend extends ScalarUdf {
    private static final long MILLI_SECONDS_IN_MINUTE = 60000;

    public double eval(ArrayList<Timestamp> timestampsArray, ArrayList<Double> values, Timestamp hopStartTime, Integer windowLengthInMinutes) {
        return calculateLinearTrend(timestampsArray, values, hopStartTime, windowLengthInMinutes);
    }

    private double calculateLinearTrend(ArrayList<Timestamp> timestampsArray, ArrayList<Double> valueList, Timestamp hopStartTime, Integer windowLengthInMinutes) {
        ArrayList<Double> hopWindowList = IntStream.range(0, windowLengthInMinutes).mapToObj(i -> (double) i).collect(Collectors.toCollection(ArrayList::new));
        ArrayList<Double> orderedValueList = getOrderedValueList(hopStartTime, valueList, timestampsArray, windowLengthInMinutes);

        double timeValueCovariance = getCovariance(hopWindowList, orderedValueList, windowLengthInMinutes);
        double timeVariance = getVariance(hopWindowList, windowLengthInMinutes);
        return (timeValueCovariance / timeVariance);
    }

    private ArrayList<Double> getOrderedValueList(Timestamp hopStartTime, ArrayList<Double> valueList, ArrayList<Timestamp> timestampsArray, int windowLengthInMinutes) {
        ArrayList<Double> orderedValueList = new ArrayList<>(Collections.nCopies(windowLengthInMinutes, 0d));
        IntStream.range(0, valueList.size()).forEach(index -> {
            double value = valueList.get(index);
            Timestamp valueStartTime = timestampsArray.get(index);
            int position = getPosition(valueStartTime, hopStartTime);
            orderedValueList.set(position, value);
        });
        return orderedValueList;
    }

    private int getPosition(Timestamp valueStartTime, Timestamp hopStartTime) {
        long hopStartMS = hopStartTime.getTime();
        long valueStartMS = valueStartTime.getTime();

        long deltaInMinute = ((valueStartMS - hopStartMS) / MILLI_SECONDS_IN_MINUTE);
        return (int) deltaInMinute;
    }

    private double getVariance(ArrayList<Double> list, int hopWindowLength) {
        return getSumOfAnArray(getSquareArray(list)) - Math.pow(getSumOfAnArray(list), 2) / hopWindowLength;
    }

    private double getCovariance(ArrayList<Double> listOne, ArrayList<Double> listTwo, int hopWindowLength) {
        return getSumOfAnArray(multiplyListsOfSameLength(listOne, listTwo)) - getSumOfAnArray(listOne) * getSumOfAnArray(listTwo) / hopWindowLength;
    }

    private ArrayList<Double> multiplyListsOfSameLength(ArrayList<Double> listOne, ArrayList<Double> listTwo) {
        ArrayList<Double> arrayAfterMultiplication = new ArrayList<>();
        IntStream.range(0, listOne.size()).forEach(index -> arrayAfterMultiplication.add(index, listOne.get(index) * listTwo.get(index)));
        return arrayAfterMultiplication;
    }

    private ArrayList<Double> getSquareArray(ArrayList<Double> array) {
        return array.stream().map(element -> element * element).collect(Collectors.toCollection(ArrayList::new));
    }

    private double getSumOfAnArray(ArrayList<Double> array) {
        return array.stream().mapToDouble(element -> element).sum();
    }
}
