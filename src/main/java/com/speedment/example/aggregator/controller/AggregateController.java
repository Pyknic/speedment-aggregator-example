package com.speedment.example.aggregator.controller;

import com.speedment.enterprise.datastore.runtime.DataStoreComponent;
import com.speedment.enterprise.datastore.runtime.aggregator.Aggregator;
import com.speedment.enterprise.datastore.runtime.entitystore.EntityStore;
import com.speedment.enterprise.datastore.runtime.entitystore.Order;
import com.speedment.enterprise.datastore.runtime.fieldcache.FieldCache;
import com.speedment.example.aggregator.db.salaries.Salary;
import com.speedment.example.aggregator.db.salaries.SalaryManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

@RestController
@RequestMapping("salaries")
public class AggregateController {

    private final static int SECONDS_IN_A_DAY = 86_400;

    private @Autowired DataStoreComponent dataStore;
    private @Autowired SalaryManager salaries;

    @GetMapping("count")
    long count() {
        return salaries.stream().count();
    }

    @GetMapping(path="correlate", produces = MediaType.APPLICATION_JSON_VALUE)
    String correlate() {

        final EntityStore<Salary> store = entityStore();
        final FieldCache.OfEnum<Salary.Gender> genders = genderFieldCache();

        return Aggregator.builder(store, MeanResult::new)
            .withEnumKey(Salary.GENDER)
            .withCount(MeanResult::setCount)
            .withAverage(Salary.SALARY, MeanResult::setMeanSalary)
            .withAverage(store2 -> ref ->
                    (store2.deserializeInt(ref, Salary.FROM_DATE) -
                        store2.deserializeInt(ref, Salary.HIRE_DATE)) / SECONDS_IN_A_DAY,
                MeanResult::setMeanDaysOfEmployment)
            .build()
            .aggregate(store.references())
            .flatMap(mean -> {
                final Salary.Gender gender = store.deserializeReference(mean.ref, Salary.GENDER);
                return Aggregator.builder(store, VarianceResult::new)
                    .withEnumKey(Salary.GENDER)
                    .withCount(VarianceResult::setCount)
                    .withAverage(store2 -> ref ->
                        store2.deserializeInt(ref, Salary.SALARY) - mean.meanSalary,
                        VarianceResult::setVarianceSalary)
                    .withAverage(store2 -> ref -> (
                        store2.deserializeInt(ref, Salary.FROM_DATE) -
                        store2.deserializeInt(ref, Salary.HIRE_DATE)) / SECONDS_IN_A_DAY -
                        mean.meanDaysOfEmployment,
                        VarianceResult::setVarianceDaysOfEmployment
                    )
                    .build()
                    .aggregate(genders.equal(gender, Order.ASC, 0, Long.MAX_VALUE))
                    .map(res -> res.setMeanSalary(mean.meanSalary))
                    .map(res -> res.setMeanDaysOfEmployment(mean.meanSalary));
            })
            .map(result -> format(
                    "\"%s\":{\"count\":%d,\"salaryMean\":%.5f,\"salaryVariance\":%.5f," +
                    "\"daysOfEmploymentMean\":%.5f,\"daysOfEmploymentVariance\":%.5f}",
                formatGender(store.deserializeReference(result.ref, Salary.GENDER)),
                result.count,
                result.meanSalary,
                result.varianceSalary,
                result.meanDaysOfEmployment,
                result.varianceDaysOfEmployment
            )).collect(joining(",", "{", "}"));
    }

    private FieldCache.OfEnum<Salary.Gender> genderFieldCache() {
        return dataStore.currentHolder().getFieldCache(
            Salary.GENDER.identifier()
        );
    }

    private EntityStore<Salary> entityStore() {
        return dataStore.currentHolder().getEntityStore(
            Salary.EMP_NO.identifier().asTableIdentifier()
        );
    }

    private static String formatGender(Salary.Gender gender) {
        switch (gender) {
            case M : return "men";
            case F : return "women";
        }
        throw new IllegalStateException();
    }

    private final static class MeanResult {
        final long ref;
        long count;
        double meanSalary;
        double meanDaysOfEmployment;

        MeanResult(long ref) {
            this.ref = ref;
        }

        void setCount(long count) {
            this.count = count;
        }

        void setMeanSalary(double meanSalary) {
            this.meanSalary = meanSalary;
        }

        void setMeanDaysOfEmployment(double meanDaysOfEmployment) {
            this.meanDaysOfEmployment = meanDaysOfEmployment;
        }
    }

    private final static class VarianceResult {
        private final long ref;
        private long count;
        private double meanSalary;
        private double meanDaysOfEmployment;
        private double varianceSalary;
        private double varianceDaysOfEmployment;

        VarianceResult(long ref) {
            this.ref = ref;
        }

        long getRef() {
            return ref;
        }

        void setCount(long count) {
            this.count = count;
        }

        VarianceResult setMeanSalary(double meanSalary) {
            this.meanSalary = meanSalary;
            return this;
        }

        VarianceResult setMeanDaysOfEmployment(double meanDaysOfEmployment) {
            this.meanDaysOfEmployment = meanDaysOfEmployment;
            return this;
        }

        void setVarianceSalary(double varianceSalary) {
            this.varianceSalary = varianceSalary;
        }

        void setVarianceDaysOfEmployment(double varianceDaysOfEmployment) {
            this.varianceDaysOfEmployment = varianceDaysOfEmployment;
        }
    }
}
