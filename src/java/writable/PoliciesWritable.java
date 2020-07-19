package java.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Unless stated otherwise all varaibles are measured on an ordinal scale from with a greater number representing a
 * stricter response. See https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/codebook.md for a
 * complete description of each field.
 */
public class PoliciesWritable implements Writable {
    // Containment policies
    int schoolClosing;
    int workPlaceClosing;
    int cancelPublicEvents;
    int restrictionsOnGatherings;
    int closePublicTransport;
    int stayAtHomeRequirments;
    int restrictionsOnInternalMovement;
    int internationalTravelControls;
    // Economic policies
    int incomeSupport;
    int debtRelief;
    int fiscalMeasures;
    int internationalSupport;
    // Health System policies
    int publicInfoCampaigns;
    int testingPolicy;
    int contactTracing;
    int emergencyInvestmentInHealthcare;
    int investmentInVaccines;
    // Miscellaneous: Unusual or interesting interventions that require flagging.
    String wildardPolicies;

    public PoliciesWritable() {
    }

    public PoliciesWritable(int schoolClosing, int workPlaceClosing, int cancelPublicEvents, int restrictionsOnGatherings, int closePublicTransport, int stayAtHomeRequirments, int restrictionsOnInternalMovement, int internationalTravelControls, int incomeSupport, int debtRelief, int fiscalMeasures, int internationalSupport, int publicInfoCampaigns, int testingPolicy, int contactTracing, int emergencyInvestmentInHealthcare, int investmentInVaccines, String wildardPolicies) {
        this.schoolClosing = schoolClosing;
        this.workPlaceClosing = workPlaceClosing;
        this.cancelPublicEvents = cancelPublicEvents;
        this.restrictionsOnGatherings = restrictionsOnGatherings;
        this.closePublicTransport = closePublicTransport;
        this.stayAtHomeRequirments = stayAtHomeRequirments;
        this.restrictionsOnInternalMovement = restrictionsOnInternalMovement;
        this.internationalTravelControls = internationalTravelControls;
        this.incomeSupport = incomeSupport;
        this.debtRelief = debtRelief;
        this.fiscalMeasures = fiscalMeasures;
        this.internationalSupport = internationalSupport;
        this.publicInfoCampaigns = publicInfoCampaigns;
        this.testingPolicy = testingPolicy;
        this.contactTracing = contactTracing;
        this.emergencyInvestmentInHealthcare = emergencyInvestmentInHealthcare;
        this.investmentInVaccines = investmentInVaccines;
        this.wildardPolicies = wildardPolicies;
    }

    public PoliciesWritable setSchoolClosing(int schoolClosing) {
        this.schoolClosing = schoolClosing;
        return this;
    }

    public PoliciesWritable setWorkPlaceClosing(int workPlaceClosing) {
        this.workPlaceClosing = workPlaceClosing;
        return this;
    }

    public PoliciesWritable setCancelPublicEvents(int cancelPublicEvents) {
        this.cancelPublicEvents = cancelPublicEvents;
        return this;
    }

    public PoliciesWritable setRestrictionsOnGatherings(int restrictionsOnGatherings) {
        this.restrictionsOnGatherings = restrictionsOnGatherings;
        return this;
    }

    public PoliciesWritable setClosePublicTransport(int closePublicTransport) {
        this.closePublicTransport = closePublicTransport;
        return this;
    }

    public PoliciesWritable setStayAtHomeRequirments(int stayAtHomeRequirments) {
        this.stayAtHomeRequirments = stayAtHomeRequirments;
        return this;
    }

    public PoliciesWritable setRestrictionsOnInternalMovement(int restrictionsOnInternalMovement) {
        this.restrictionsOnInternalMovement = restrictionsOnInternalMovement;
        return this;
    }

    public PoliciesWritable setInternationalTravelControls(int internationalTravelControls) {
        this.internationalTravelControls = internationalTravelControls;
        return this;
    }

    public PoliciesWritable setIncomeSupport(int incomeSupport) {
        this.incomeSupport = incomeSupport;
        return this;
    }

    public PoliciesWritable setDebtRelief(int debtRelief) {
        this.debtRelief = debtRelief;
        return this;
    }

    public PoliciesWritable setFiscalMeasures(int fiscalMeasures) {
        this.fiscalMeasures = fiscalMeasures;
        return this;
    }

    public PoliciesWritable setInternationalSupport(int internationalSupport) {
        this.internationalSupport = internationalSupport;
        return this;
    }

    public PoliciesWritable setPublicInfoCampaigns(int publicInfoCampaigns) {
        this.publicInfoCampaigns = publicInfoCampaigns;
        return this;
    }

    public PoliciesWritable setTestingPolicy(int testingPolicy) {
        this.testingPolicy = testingPolicy;
        return this;
    }

    public PoliciesWritable setContactTracing(int contactTracing) {
        this.contactTracing = contactTracing;
        return this;
    }

    public PoliciesWritable setEmergencyInvestmentInHealthcare(int emergencyInvestmentInHealthcare) {
        this.emergencyInvestmentInHealthcare = emergencyInvestmentInHealthcare;
        return this;
    }

    public PoliciesWritable setInvestmentInVaccines(int investmentInVaccines) {
        this.investmentInVaccines = investmentInVaccines;
        return this;
    }

    public PoliciesWritable setWildardPolicies(String wildardPolicies) {
        this.wildardPolicies = wildardPolicies;
        return this;
    }

    public PoliciesWritable build() {
        return new PoliciesWritable(schoolClosing, workPlaceClosing, cancelPublicEvents, restrictionsOnGatherings,
                closePublicTransport, stayAtHomeRequirments, restrictionsOnInternalMovement, internationalTravelControls,
                incomeSupport, debtRelief, fiscalMeasures, internationalSupport, publicInfoCampaigns, testingPolicy,
                contactTracing, emergencyInvestmentInHealthcare, investmentInVaccines, wildardPolicies);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(schoolClosing);
        out.write(workPlaceClosing);
        out.write(cancelPublicEvents);
        out.write(restrictionsOnGatherings);
        out.write(closePublicTransport);
        out.write(stayAtHomeRequirments);
        out.write(restrictionsOnInternalMovement);
        out.write(internationalTravelControls);
        out.write(incomeSupport);
        out.write(debtRelief);
        out.write(fiscalMeasures);
        out.write(internationalSupport);
        out.write(publicInfoCampaigns);
        out.write(testingPolicy);
        out.write(contactTracing);
        out.write(emergencyInvestmentInHealthcare);
        out.write(investmentInVaccines);
        out.writeUTF(wildardPolicies);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        schoolClosing = in.readInt();
        workPlaceClosing = in.readInt();
        cancelPublicEvents = in.readInt();
        restrictionsOnGatherings = in.readInt();
        closePublicTransport = in.readInt();
        stayAtHomeRequirments = in.readInt();
        restrictionsOnInternalMovement = in.readInt();
        internationalTravelControls = in.readInt();
        incomeSupport = in.readInt();
        debtRelief = in.readInt();
        fiscalMeasures = in.readInt();
        internationalSupport = in.readInt();
        publicInfoCampaigns = in.readInt();
        testingPolicy = in.readInt();
        contactTracing = in.readInt();
        emergencyInvestmentInHealthcare = in.readInt();
        investmentInVaccines = in.readInt();
        wildardPolicies = in.readUTF();
    }
}