package com.speedment.example.aggregator.controller;

import com.speedment.enterprise.datastore.runtime.DataStoreComponent;
import com.speedment.enterprise.datastore.runtime.aggregator.Aggregator;
import com.speedment.enterprise.datastore.runtime.entitystore.EntityStore;
import com.speedment.enterprise.datastore.runtime.entitystore.Order;
import com.speedment.enterprise.datastore.runtime.fieldcache.FieldCache;
import com.speedment.enterprise.datastore.runtime.function.deserialize.DeserializeDouble;
import com.speedment.example.aggregator.db.salaries.Salary;
import com.speedment.example.aggregator.db.salaries.SalaryManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.speedment.enterprise.datastore.runtime.aggregator.Aggregators.*;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

@RestController
@RequestMapping("salaries")
public class AggregateController {

    private final static int SECONDS_IN_A_DAY = 86_400;
    private final static DeserializeDouble<Salary> DAYS_EMPLOYED =
        multiply(
            minus(
                getAsLongOrElse(Salary.FROM_DATE, 0),
                getAsLongOrElse(Salary.HIRE_DATE, 0)
            ).asDeserializeDouble(),
            fixed(1d / SECONDS_IN_A_DAY)
        );

    private @Autowired DataStoreComponent dataStore;
    private @Autowired SalaryManager salaries;

    @GetMapping(path="correlate", produces = MediaType.APPLICATION_JSON_VALUE)
    String correlate() {

        final EntityStore<Salary> store = entityStore();
        final FieldCache.OfEnum<Salary.Gender> genders = genderFieldCache();

        // In the first pass, compute the mean salary and days of employment
        return Aggregator.builder(store, FirstPass::new)
            .withEnumKey(Salary.GENDER)
            .withCount(FirstPass::setCount)
            .withAverage(Salary.SALARY, FirstPass::setSalaryMean)
            .withAverage(DAYS_EMPLOYED, FirstPass::setDaysEmployedMean)
            .build()
            .aggregate(store.references())

            // As a second pass, compute the variance of salary, days of employment and their covariance
            .flatMap(mean -> {
                final Salary.Gender gender = store.deserializeReference(mean.ref, Salary.GENDER);
                return Aggregator.builder(store, ref -> new SecondPass(ref, mean.salaryMean, mean.daysEmployedMean))
                    .withEnumKey(Salary.GENDER)
                    .withCount(SecondPass::setCount)
                    .withVariance(Salary.SALARY, mean.salaryMean, SecondPass::setSalaryVariance)
                    .withVariance(DAYS_EMPLOYED, mean.daysEmployedMean, SecondPass::setDaysEmployedVariance)
                    .withAverage(multiply(
                        minus(
                            getAsDouble(Salary.SALARY),
                            fixed(mean.salaryMean)
                        ),
                        minus(
                            DAYS_EMPLOYED,
                            fixed(mean.daysEmployedMean)
                        )
                    ), SecondPass::setCovariance)
                    .build()
                    .aggregate(genders.equal(gender, Order.ASC, 0, Long.MAX_VALUE));
            })

            // Compute the correlation coefficient from the covariance and variance
            .map(SecondPass::computeCorrelation)

            // Collect the result as json.
            .map(result -> format(
                    "\"%s\":{\"count\":%d,\"salaryMean\":%.2f,\"salaryVariance\":%.5f," +
                    "\"daysEmployedMean\":%.2f,\"daysEmployedVariance\":%.5f,\"covariance\":%.5f,\"correlation\":%.5f}",
                formatGender(store.deserializeReference(result.ref, Salary.GENDER)),
                result.count,
                result.salaryMean,
                result.salaryVariance,
                result.daysEmployedMean,
                result.daysEmployedVariance,
                result.covariance,
                result.correlation
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

    private final static class FirstPass {
        final long ref;
        long count;
        double salaryMean;
        double daysEmployedMean;

        FirstPass(long ref) {
            this.ref = ref;
        }

        void setCount(long count) {
            this.count = count;
        }

        void setSalaryMean(double salaryMean) {
            this.salaryMean = salaryMean;
        }

        void setDaysEmployedMean(double meanDaysOfEmployment) {
            this.daysEmployedMean = meanDaysOfEmployment;
        }
    }

    private final static class SecondPass {

        private final long ref;
        private long count;
        private double salaryMean;
        private double daysEmployedMean;
        private double salaryVariance;
        private double daysEmployedVariance;
        private double covariance;
        private double correlation;

        SecondPass(long ref, double salaryMean, double daysEmployedMean) {
            this.ref = ref;
            this.salaryMean = salaryMean;
            this.daysEmployedMean = daysEmployedMean;
        }

        void setCount(long count) {
            this.count = count;
        }

        void setSalaryVariance(double varianceSalary) {
            this.salaryVariance = varianceSalary;
        }

        void setDaysEmployedVariance(double daysEmployedVariance) {
            this.daysEmployedVariance = daysEmployedVariance;
        }

        void setCovariance(double covariance) {
            this.covariance = covariance;
        }

        SecondPass computeCorrelation() {
            if (salaryVariance == 0 || daysEmployedVariance == 0) {
                this.correlation = Double.NaN;
            } else {
                this.correlation = covariance / (
                    Math.sqrt(salaryVariance) *
                    Math.sqrt(daysEmployedVariance)
                );
            }
            return this;
        }
    }
}
