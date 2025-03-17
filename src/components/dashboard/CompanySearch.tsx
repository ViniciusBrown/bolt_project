import React, { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Search } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command';
import { CompanySentiment } from '@/types';

interface CompanySearchProps {
  companies: CompanySentiment[];
  onSelectCompany: (company: CompanySentiment) => void;
  currentCompany: CompanySentiment;
}

export function CompanySearch({ companies, onSelectCompany, currentCompany }: CompanySearchProps) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" className="w-full justify-start">
          <Search className="mr-2 h-4 w-4" />
          <span>{currentCompany.company}</span>
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>Search Companies</DialogTitle>
        </DialogHeader>
        <Command>
          <CommandInput placeholder="Search company..." />
          <CommandList>
            <CommandEmpty>No companies found.</CommandEmpty>
            <CommandGroup>
              {companies.map((company) => (
                <CommandItem
                  key={company.company}
                  onSelect={() => {
                    onSelectCompany(company);
                    setOpen(false);
                  }}
                >
                  <div className="flex items-center">
                    <img 
                      src={company.logo_url} 
                      alt={company.company} 
                      className="h-6 w-6 rounded-full mr-2"
                    />
                    {company.company}
                  </div>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </DialogContent>
    </Dialog>
  );
}
