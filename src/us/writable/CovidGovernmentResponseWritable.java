package us.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * See https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/codebook.md for a
 * complete description of each field.
 */
public class CovidGovernmentResponseWritable implements Writable {
    // Either USA (United States) or CHN(China).
    String countryCode;
    // YYYY-MM-DD
    String date;
    int confirmedCases;
    int deaths;
    // Government Response Stringency Index scared from 0 - 100 (100 = strictest response)
    double stringencyActual;
    // If data is unavailable for given date, holds the previous day data.
    double stringency;
    // Legacy calculation of stringency used until 2020-04-28
    double stringencyLegacy;
    // The 3 major parts of the stringency index.
    double governmentResponseIndex;
    double containmentHealthIndex;
    double economicSupportIndex;

    // 17 indicators used to calculate the stringencyIndex.
    PoliciesWritable policies;

    public CovidGovernmentResponseWritable() {
    }

    public CovidGovernmentResponseWritable(String countryCode, String date, int confirmedCases, int deaths,
                                           double stringencyActual, double stringency, double stringencyLegacy,
                                           double governmentResponseIndex, double containmentHealthIndex,
                                           double economicSupportIndex, PoliciesWritable policies) {
        this.countryCode = countryCode;
        this.date = date;
        this.confirmedCases = confirmedCases;
        this.deaths = deaths;
        this.stringencyActual = stringencyActual;
        this.stringency = stringency;
        this.stringencyLegacy = stringencyLegacy;
        this.governmentResponseIndex = governmentResponseIndex;
        this.containmentHealthIndex = containmentHealthIndex;
        this.economicSupportIndex = economicSupportIndex;
        this.policies = policies;
    }

    public CovidGovernmentResponseWritable setCountryCode(String countryCode) {
        this.countryCode = countryCode;
        return this;
    }

    public CovidGovernmentResponseWritable setDate(String date) {
        this.date = date;
        return this;
    }

    public CovidGovernmentResponseWritable setConfirmedCases(int confirmedCases) {
        this.confirmedCases = confirmedCases;
        return this;
    }

    public CovidGovernmentResponseWritable setDeaths(int deaths) {
        this.deaths = deaths;
        return this;
    }

    public CovidGovernmentResponseWritable setStringencyActual(double stringencyActual) {
        this.stringencyActual = stringencyActual;
        return this;
    }

    public CovidGovernmentResponseWritable setStringency(double stringency) {
        this.stringency = stringency;
        return this;
    }

    public CovidGovernmentResponseWritable setStringencyLegacy(double stringencyLegacy) {
        this.stringencyLegacy = stringencyLegacy;
        return this;
    }

    public CovidGovernmentResponseWritable setGovernmentResponseIndex(double governmentResponseIndex) {
        this.governmentResponseIndex = governmentResponseIndex;
        return this;
    }

    public CovidGovernmentResponseWritable setContainmentHealthIndex(double containmentHealthIndex) {
        this.containmentHealthIndex = containmentHealthIndex;
        return this;
    }

    public CovidGovernmentResponseWritable setEconomicSupportIndex(double economicSupportIndex) {
        this.economicSupportIndex = economicSupportIndex;
        return this;
    }

    public CovidGovernmentResponseWritable setPolicies(PoliciesWritable policies) {
        this.policies = policies;
        return this;
    }

    public CovidGovernmentResponseWritable build() {
        return new CovidGovernmentResponseWritable(countryCode, date, confirmedCases, deaths, stringencyActual,
                stringency, stringencyLegacy, governmentResponseIndex, containmentHealthIndex, economicSupportIndex,
                policies);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(countryCode);
        out.writeUTF(date);
        out.write(confirmedCases);
        out.write(deaths);
        out.writeDouble(stringencyActual);
        out.writeDouble(stringency);
        out.writeDouble(stringencyLegacy);
        out.writeDouble(governmentResponseIndex);
        out.writeDouble(containmentHealthIndex);
        out.writeDouble(economicSupportIndex);
        policies.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        countryCode = in.readUTF();
        date = in.readUTF();
        confirmedCases = in.readInt();
        deaths = in.readInt();
        stringencyActual = in.readInt();
        stringency = in.readInt();
        stringencyLegacy = in.readInt();
        governmentResponseIndex = in.readInt();
        containmentHealthIndex = in.readInt();
        economicSupportIndex = in.readInt();
        policies.readFields(in);

    }
}

