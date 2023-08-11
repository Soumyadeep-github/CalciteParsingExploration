package inprogress.ApacheVelocity.dcrTemplates;

public class OnboardDatasetDTO {
    Boolean isOwner;
    OnboardDatasetDTO(Boolean isOwner){
        this.isOwner = isOwner;
    }
    public boolean getIsOwner() {
        return this.isOwner;
    }
}
